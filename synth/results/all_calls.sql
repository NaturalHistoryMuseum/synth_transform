select
    r.name,
    c.start,
    c.end
from `Call` c
join Round r on c.round_id = r.id;
