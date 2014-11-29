SET bdr.skip_ddl_replication = on;
SET bdr.permit_unsafe_ddl_commands = on;

CREATE OR REPLACE FUNCTION bdr.bdr_sequencer_vote(p_sysid text, p_tlid oid, p_dboid oid, p_riname text)
RETURNS int
VOLATILE
LANGUAGE plpgsql
AS $body$
DECLARE
    v_curel record;
    v_curvote bool;
    v_ourvote_failed bool;
    v_nvotes int := 0;
BEGIN
    FOR v_curel IN SELECT *
        FROM bdr_sequence_elections election
        WHERE
            election.open
            -- not our election
            AND NOT (
                owning_sysid = p_sysid
                AND owning_tlid = p_tlid
                AND owning_dboid = p_dboid
                AND owning_riname = p_riname
            )
            -- we haven't voted about this yet
            AND NOT EXISTS (
                SELECT *
                FROM bdr_votes
                WHERE true
                    AND owning_sysid = vote_sysid
                    AND owning_tlid = vote_tlid
                    AND owning_dboid = vote_dboid
                    AND owning_riname = vote_riname
                    AND owning_election_id = vote_election_id

                    AND voter_sysid = p_sysid
                    AND voter_tlid = p_tlid
                    AND voter_dboid = p_dboid
                    AND voter_riname = p_riname
            )
        LOOP

        v_ourvote_failed = false;

        -- We haven't allowed anybody else to use it.
        IF EXISTS(
            SELECT *
            FROM bdr_sequence_elections other_election
                JOIN bdr_votes vote ON (
                    other_election.owning_sysid = vote.vote_sysid
                    AND other_election.owning_tlid = vote.vote_tlid
                    AND other_election.owning_dboid = vote.vote_dboid
                    AND other_election.owning_riname = vote.vote_riname
                    AND other_election.owning_election_id = vote.vote_election_id
                )
            WHERE true
                AND vote.voter_sysid = p_sysid
                AND vote.voter_tlid = p_tlid
                AND vote.voter_dboid = p_dboid
                AND vote.voter_riname = p_riname
                AND other_election.seqname = v_curel.seqname
                AND other_election.seqschema = v_curel.seqschema
                AND other_election.seqrange && v_curel.seqrange
        ) THEN
            v_curvote = false;
        -- If we already got the chunk, it's over
        ELSEIF EXISTS(
            SELECT *
            FROM bdr_sequence_values val
            WHERE true
                AND val.confirmed
                AND val.seqschema = v_curel.seqschema
                AND val.seqname = v_curel.seqname
                AND val.seqrange && v_curel.seqrange
                AND val.owning_sysid = p_sysid
                AND val.owning_tlid = p_tlid
                AND val.owning_dboid = p_dboid
                AND val.owning_riname = p_riname
        ) THEN
            v_curvote = false;
        -- If we have allocated the value ourselves, check whether we
        -- should be allowed, or whether we want to allow the other
        -- guy.
        ELSEIF EXISTS(
            SELECT *
            FROM bdr_sequence_values val
            WHERE true
                AND NOT val.confirmed
                AND val.seqschema = v_curel.seqschema
                AND val.seqname = v_curel.seqname
                AND val.seqrange && v_curel.seqrange
                AND val.owning_sysid = p_sysid
                AND val.owning_tlid = p_tlid
                AND val.owning_dboid = p_dboid
                AND val.owning_riname = p_riname
        ) THEN
            /* allow the guy with the smaller (sysid, tlid, dboid, riname) pair */
            IF (p_sysid, p_tlid, p_dboid, p_riname) <
               (v_curel.owning_sysid,
                v_curel.owning_tlid,
                v_curel.owning_dboid,
                v_curel.owning_riname)
            THEN
                /* this side wins */
                v_curvote = false;
            ELSE
                /* other side wins*/
                v_curvote = true;

                /*
                 * Delay update to after the insertion into bdr_votes so we
                 * have consistent lock acquiration order with
                 * bdr_sequencer_lock().
                 */

                v_ourvote_failed = true;
            END IF;
        ELSE
            v_curvote = true;
        END IF;

        /* now actually do the vote */
        INSERT INTO bdr_votes (
            vote_sysid,
            vote_tlid,
            vote_dboid,
            vote_riname,
            vote_election_id,

            voter_sysid,
            voter_tlid,
            voter_dboid,
            voter_riname,
            vote
        )
        VALUES
        (
            v_curel.owning_sysid,
            v_curel.owning_tlid,
            v_curel.owning_dboid,
            v_curel.owning_riname,
            v_curel.owning_election_id,
            p_sysid,
            p_tlid,
            p_dboid,
            p_riname,
            v_curvote
        );

        IF v_ourvote_failed THEN
            UPDATE bdr.bdr_sequence_elections AS ourel
            SET open = false, success = false
            WHERE true
                AND ourel.seqschema = v_curel.seqschema
                AND ourel.seqname = v_curel.seqname
                AND ourel.seqrange && v_curel.seqrange
                AND ourel.owning_sysid = p_sysid
                AND ourel.owning_tlid = p_tlid
                AND ourel.owning_dboid = p_dboid
                AND ourel.owning_riname = p_riname;

            UPDATE bdr.bdr_sequence_values AS ourchunk
            SET failed = true
            WHERE true
                AND ourchunk.seqschema = v_curel.seqschema
                AND ourchunk.seqname = v_curel.seqname
                AND ourchunk.seqrange && v_curel.seqrange
                AND ourchunk.owning_sysid = p_sysid
                AND ourchunk.owning_tlid = p_tlid
                AND ourchunk.owning_dboid = p_dboid
                AND ourchunk.owning_riname = p_riname;
        END IF;

        v_nvotes = v_nvotes + 1;
    END LOOP;

    RETURN v_nvotes;
END
$body$;
REVOKE ALL ON FUNCTION bdr.bdr_sequencer_vote(p_sysid text, p_tlid oid, p_dboid oid, p_riname text) FROM public;
