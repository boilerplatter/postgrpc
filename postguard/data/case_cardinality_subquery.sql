select
  tree.id,
  tree.name,
  case
    when cardinality(array_remove(array_agg(activations), null)) > 0
    then bool_or(activations.deactivated_at is null)
    else false
  end as active,
  (
    select name
    from branches
    where branches.id = tree.trunk_branch_id
  ) as trunk_branch_name
from tree
  left join branches on branches.virtualized_instance_id = tree.id
  left join activations on activations.branch_id = branches.id
where user_id = $1
group by tree.id
