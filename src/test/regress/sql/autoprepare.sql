select count(*) from pg_class where relname='pg_class';
select * from pg_autoprepared_statements;
set autoprepare_threshold = 1;
select count(*) from pg_class where relname='pg_class';
select * from pg_autoprepared_statements;
select count(*) from pg_class where relname='pg_class';
select * from pg_autoprepared_statements;
set autoprepare_threshold = 0;
select * from pg_autoprepared_statements;
select count(*) from pg_class where relname='pg_class';
select * from pg_autoprepared_statements;
