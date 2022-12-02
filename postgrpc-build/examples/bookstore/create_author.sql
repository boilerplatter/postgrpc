insert into authors (first_name, last_name)
values ($1, $2)
returning *;
