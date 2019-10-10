customers = LOAD 'input/customer.txt' USING PigStorage(',') AS (ID, Name, Age, Gender, CountryCode, Salary);
A = Group customers by (CountryCode);

B = FOREACH A generate group AS (CountryCode), COUNT(customers) as cnt;
C = filter B by cnt > 5000;
D = filter B by cnt < 2000;
E = UNION C, D;
store E into 'output/PigQuery3.txt';
