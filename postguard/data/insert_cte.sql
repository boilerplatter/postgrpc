with cte as (
    insert into items (id, name)
    values ($1, $2)
    returning id
)
select id from cte
