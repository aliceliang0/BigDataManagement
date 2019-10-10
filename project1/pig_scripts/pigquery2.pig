transactions = LOAD 'input/transactions.txt' USING PigStorage(',') AS(TransID, CustID, TransTotal, TransNumItems, TransDesc);
customers = LOAD 'input/customer.txt' USING PigStorage(',') AS (ID, Name, Age, Gender, CountryCode, Salary);
C = Group transactions by (CustID);
D = FOREACH C generate group AS CustID, COUNT(transactions.TransID) As NumOfTransactions, SUM(transactions.TransTotal) As TotalSum, MIN(transactions.TransNumItems) As MinItems;
E = join D by CustID, customers by ID using 'replicated';

F = Foreach E generate CustID, Name, Salary, NumOfTransactions, TotalSum, MinItems;
store F into 'output/PigQuery2.txt' USING PigStorage(',');
