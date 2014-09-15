ALTER TYPE bdr_conflict_resolution ADD VALUE 'apply_change' BEFORE 'unhandled_tx_abort';
ALTER TYPE bdr_conflict_resolution ADD VALUE 'skip_change' BEFORE 'unhandled_tx_abort';
