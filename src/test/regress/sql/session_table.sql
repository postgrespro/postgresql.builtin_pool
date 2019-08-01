create session table my_private_table(x integer primary key, y integer);
insert into my_private_table values (generate_series(1,10000), generate_series(1,10000));
select count(*) from my_private_table;
\c
select count(*) from my_private_table;
select * from my_private_table where x=10001;
insert into my_private_table values (generate_series(1,100000), generate_series(1,100000));
create index on my_private_table(y);
select * from my_private_table where x=10001;
select * from my_private_table where y=10001;
select count(*) from my_private_table;
\c
select * from my_private_table where x=100001;
select * from my_private_table order by y desc limit 1;
insert into my_private_table values (generate_series(1,100000), generate_series(1,100000));
select * from my_private_table where x=100001;
select * from my_private_table order by y desc limit 1;
drop table  my_private_table;
