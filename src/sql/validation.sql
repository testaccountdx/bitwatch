--verify transaction uniqueness by returning all duplicate transactions (should be zero)
SELECT txid, COUNT(*) FROM transactionsmvp GROUP BY txid HAVING count(*) > 0;





