--criar banco db

create database db;

---Criar a tabela transactions.

create table transactions (
    id int unsigned not null auto_increment, 
    step int,
    customer varchar(12),
    age int,
    gender char(1),
    zipCodeOri varchar(10),
    merchant varchar(11),
    zipMerchant varchar(10),
    category varchar(25),
    amount double,
    fraud int,
    month int,
    year int,
    constraint transactions_id primary key (id)
)

--criar banco analytic


create database analytic;

use analytic;

create table fact_merchant_kpi (
    data date,
	id_merchant varchar (11),
    tpv double,
    qtd_transacoes int
)

create table fact_customer_kpi (
    data date,
	id_customer varchar (12),
    tpv double,
    qtd_transacoes int
);


create table merchant_category (
    id_merchant varchar (11),
    category varchar (25),
    constraint pk_merchant primary key (id_merchant)
);

--procedure de inserção de dados
CREATE PROCEDURE sp_analytic_data()
insert into analytic.fact_merchant_kpi (
select concat(year,'/',month,'/1') as data,
merchant as id_merchant,
sum(amount) as tpv,
count(distinct(id)) as qtd_transacoes
from db.transactions group by data, merchant);
insert into analytic.fact_customer_kpi (
select concat(year,'/',month,'/1') as data,
customer as id_customer,
sum(amount) as tpv,
count(distinct(id)) as qtd_transacoes
from db.transactions group by data, customer);
insert into analytic.merchant_category (
select 
merchant,
category
from db.transactions group by merchant);



