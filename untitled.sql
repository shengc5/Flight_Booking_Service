create table customer(username varchar(10) primary key, fullName varchar(20), zipcode int, password varchar(20));
insert into customer values('john5', 'johnBlake', 98105, 'john5123')
insert into customer values('abba3', 'abBA', 12345, 'abba123')
insert into customer values('bcde765', 'bcDE', 10101, 'bcde765')
insert into customer values('Jack788', 'JackJohns', 30165, 'jackJ')
insert into customer values('KobeB', 'KobeBryant', 17470, 'kobeMamba')
insert into customer values('shengc5', 'ShengChen', 123123, 'acbcs')
insert into customer values('guozhi11', 'guozhi', 747725, 'guozhi11y')
insert into customer values('nnj411', 'nannanJ', 09876, 'nannan')
insert into customer values('a', 'johnBlake', 98105, 's')


ALTER TABLE flights
ADD capacity INT NOT NULL DEFAULT(0)


-- drop table reservations
create table reservations(rid int primary key, username varchar(10) references customer, fid int references flights, day_of_month int, CONSTRAINT oneTic UNIQUE (username, fid))
insert into reservations values(1, 'a', 1, 0)
insert into reservations values(2, 'KobeB', 15, 0)
insert into reservations values(1, 'nnj411', 10, 0)
insert into reservations values(1, 'abba3', 12, 0)
insert into reservations values(1, 'a', 2, 0)
insert into reservations values(1, 'a', 3, 0)
insert into reservations values(1, 'a', 4, 1)



