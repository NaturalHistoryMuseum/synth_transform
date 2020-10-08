select
    r.name            `synth round`,
    count(*)          `count`
from VisitorProject vp
    join `Call` c on vp.call_submitted = c.id
    join Round r on c.round_id = r.id
where vp.length_of_visit > 0
group by r.name
order by r.name;
