insert into account (name, type, id) values ('Stock', 'S', 1);
insert into account (name, type, id) values ('Cash', 'C', 2);
insert into account_holder (account_id, name, id) values (1, 'Holder1', 1);
insert into account_holder (account_id, name, id) values (1, 'Holder2', 2);
insert into account_holder (account_id, name, id) values (2, 'Holder3', 3);
insert into position (account_id, instrument_id, price, quantity, id) values (1, 5, 63.6, 400, 1);
insert into position (account_id, instrument_id, price, quantity, id) values (2, 12, 14.5, 1000, 2);
commit;