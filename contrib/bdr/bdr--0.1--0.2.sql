CREATE TABLE bdr_sequence_values
(
    owning_sysid text NOT NULL,
    owning_tlid oid NOT NULL,
    owning_dboid bigint NOT NULL,
    owning_riname name NOT NULL,

    seqschema name NOT NULL,
    seqname name NOT NULL,
    seqrange int8range NOT NULL,

    confirmed bool NOT NULL,
    empty bool NOT NULL CHECK(NOT empty OR NOT confirmed),

    EXCLUDE USING gist(seqrange WITH &&) WHERE (confirmed)
);

CREATE INDEX bdr_sequence_values_chunks ON bdr_sequence_values(seqschema, seqname, seqrange);
CREATE INDEX bdr_sequence_values_newchunk ON bdr_sequence_values(seqschema, seqname, upper(seqrange));

CREATE TABLE bdr_sequence_elections
(
    owning_sysid text NOT NULL,
    owning_tlid oid NOT NULL,
    owning_dboid bigint NOT NULL,
    owning_riname name NOT NULL,
    owning_election_id bigint NOT NULL,

    seqschema name NOT NULL,
    seqname name NOT NULL,
    seqrange int8range NOT NULL,

    /* XXX id */

    vote_type text NOT NULL,

    open bool NOT NULL
);


CREATE TABLE bdr_votes
(
    vote_sysid text NOT NULL,
    vote_tlid oid NOT NULL,
    vote_dboid bigint NOT NULL,
    vote_riname name NOT NULL,
    vote_election_id bigint NOT NULL,

    voter_sysid text NOT NULL,
    voter_tlid oid NOT NULL,
    voter_dboid bigint NOT NULL,
    voter_riname name NOT NULL,

    vote bool NOT NULL,
    reason text CHECK (reason IS NULL OR vote = false),
    UNIQUE(vote_sysid, vote_tlid, vote_dboid, vote_riname, vote_election_id, voter_sysid, voter_tlid, voter_dboid, voter_riname)
);
