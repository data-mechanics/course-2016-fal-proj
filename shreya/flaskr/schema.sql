--add in information and pics
drop table if exists entries;
create table entries (
  id integer primary key autoincrement,
  'text' text not null,
  title text not null,
  image_name text not null,
  pscore1 float not null,
  pscore2 float not null,
  pscore3 float not null
);

insert into entries (title, 'text', image_name, pscore1, pscore2, pscore3)
	values ('earnings','hey','earnings.png',0.807,0,0.0345);