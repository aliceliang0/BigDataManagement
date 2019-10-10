transactions = LOAD 'input/transactions.txt' USING PigStorage(',') AS(TransID:int, CustID:int, TransTotal:float, TransNumItems:int, TransDesc:chararray);
A = Group transactions by CustID;
small_transactions = FOREACH A GENERATE group AS CustID, SUM(transactions.TransTotal) as Sum;
customers = LOAD 'input/customer.txt' USING PigStorage(',') AS (ID:int, Name:chararray, Age:int, Gender:chararray, CountryCode:int, Salary:float);
Joined = JOIN small_transactions by CustID, customers by ID using 'replicated';

SPLIT Joined into Age1M if (Age>=10 and Age<20 and Gender == 'male'), Age1F if (Age>=10 and Age<20 and Gender == 'female'), Age2M if (Age>=20 and Age < 30 and Gender == 'male'), Age2F if (Age>=20 and Age < 30 and Gender == 'female'), Age3M if (Age >=30 And Age <40 and Gender == 'male'), Age3F if (Age >=30 And Age <40 and Gender == 'female'), Age4M if (Age>=40 and Age<50 and Gender == 'male'), Age4F if (Age>=40 and Age<50 and Gender == 'female'), Age5M if (Age>=50 and Age <60 and Gender == 'male'), Age5F if (Age>=50 and Age <60 and Gender == 'female'), Age6M if(Age>=60 and Age<=70 and Gender == 'male'), Age6F if(Age>=60 and Age<=70 and Gender == 'female');

Age1F_G = Group Age1F ALL;
Age1F_F = FOREACH Age1F_G Generate '[10-20)', 'female', MIN(Age1F.Sum) AS MinTransTotal, MAX(Age1F.Sum) AS MaxTransTotal, AVG(Age1F.Sum) As AveTransTotal;
Age1M_G = Group Age1M ALL;
Age1M_F = FOREACH Age1M_G Generate '[10-20)', 'male', MIN(Age1M.Sum) AS MinTransTotal, MAX(Age1M.Sum) AS MaxTransTotal, AVG(Age1M.Sum) As AveTransTotal;

Age2F_G = Group Age2F ALL;
Age2F_F = FOREACH Age2F_G Generate '[20-30)', 'female', MIN(Age2F.Sum) AS MinTransTotal, MAX(Age2F.Sum) AS MaxTransTotal, AVG(Age2F.Sum) As AveTransTotal;
Age2M_G = Group Age2M ALL;
Age2M_F = FOREACH Age2M_G Generate '[20-30)', 'male', MIN(Age2M.Sum) AS MinTransTotal, MAX(Age2M.Sum) AS MaxTransTotal, AVG(Age2M.Sum) As AveTransTotal;

Age3F_G = Group Age3F ALL;
Age3F_F = FOREACH Age3F_G Generate '[30-40)', 'female', MIN(Age3F.Sum) AS MinTransTotal, MAX(Age3F.Sum) AS MaxTransTotal, AVG(Age3F.Sum) As AveTransTotal;
Age3M_G = Group Age3M ALL;
Age3M_F = FOREACH Age3M_G Generate '[30-40)', 'male', MIN(Age3M.Sum) AS MinTransTotal, MAX(Age3M.Sum) AS MaxTransTotal, AVG(Age3M.Sum) As AveTransTotal;

Age4F_G = Group Age4F ALL;
Age4F_F = FOREACH Age4F_G Generate '[40-50)', 'female', MIN(Age4F.Sum) AS MinTransTotal, MAX(Age4F.Sum) AS MaxTransTotal, AVG(Age4F.Sum) As AveTransTotal;
Age4M_G = Group Age4M ALL;
Age4M_F = FOREACH Age4M_G Generate '[40-50)', 'male', MIN(Age4M.Sum) AS MinTransTotal, MAX(Age4M.Sum) AS MaxTransTotal, AVG(Age4M.Sum) As AveTransTotal;

Age5F_G = Group Age5F ALL;
Age5F_F = FOREACH Age5F_G Generate '[50-60)', 'female', MIN(Age5F.Sum) AS MinTransTotal, MAX(Age5F.Sum) AS MaxTransTotal, AVG(Age5F.Sum) As AveTransTotal;
Age5M_G = Group Age5M ALL;
Age5M_F = FOREACH Age5M_G Generate '[50-60)', 'male', MIN(Age5M.Sum) AS MinTransTotal, MAX(Age5M.Sum) AS MaxTransTotal, AVG(Age5M.Sum) As AveTransTotal;

Age6F_G = Group Age6F ALL;
Age6F_F = FOREACH Age6F_G Generate '[60-70]', 'female', MIN(Age6F.Sum) AS MinTransTotal, MAX(Age6F.Sum) AS MaxTransTotal, AVG(Age6F.Sum) As AveTransTotal;
Age6M_G = Group Age6M ALL;
Age6M_F = FOREACH Age6M_G Generate '[60-70]', 'male', MIN(Age6M.Sum) AS MinTransTotal, MAX(Age6M.Sum) AS MaxTransTotal, AVG(Age6M.Sum) As AveTransTotal;


FinalJoin = Union Age1F_F, Age1M_F, Age2F_F, Age2M_F, Age3F_F, Age3M_F, Age4F_F, Age4M_F, Age5F_F, Age5M_F, Age6F_F, Age6M_F;

store FinalJoin into 'output/PigQuery4.txt' USING PigStorage(',');
