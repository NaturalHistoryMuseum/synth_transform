select
    r.name    `synth round`,
    vp.gender `age range`,
    count(*)  `count`
from VisitorProject vp
    join `Call` c on vp.call_submitted = c.id
    join Round r on c.round_id = r.id
where vp.length_of_visit > 0
group by r.name, vp.gender
order by r.name, vp.gender;
