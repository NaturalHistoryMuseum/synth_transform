select r.name   `synth round`,
       cc.name  `visitor nationality country name`,
       cc.code  `visitor nationality country code`,
       count(*) `count`
from VisitorProject vp
         join `Call` c on vp.call_submitted = c.id
         join Round r on c.round_id = r.id
         join Country cc on vp.nationality = cc.id
where vp.length_of_visit > 0
group by r.name, vp.nationality
order by r.name, vp.nationality;
