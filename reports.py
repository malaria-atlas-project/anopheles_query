from sqlalchemy.orm import join
from sqlalchemy.sql import func, exists, and_, not_
from models import Anopheline, Site, SamplePeriod, Session
from sqlalchemygeom import *

session = Session(autocommit=True)

reports = []

class RawQuery(object):
    """
    Emulates the behaviour of session.query
    used to enable lazy querying of raw sql
    """
    def __init__(self, session, sql):
        self.session = session
        self.sql = sql

    def all(self):
        result_proxy = self.session.execute(self.sql)
        return result_proxy.fetchall()

class ExcelReport(object):
    """
    Just encapsulates everything needed to make an excel report
    """
    def __init__(self, title, query, headers=None, totals=None):
        #string queries
        if type(query) in (unicode, str):
            self.query = RawQuery(session, query)
        else:
            self.query = query
        self.title = title
        self.headers = headers
        self.totals = totals

sampleperiod_subq = session.query(
    SamplePeriod.anopheline2_id,
    func.count(func.distinct(SamplePeriod.site_id)).label('site_count'),
    func.count('*').label('sampleperiod_count')
    ).group_by(SamplePeriod.anopheline2_id).subquery()

point_subq = session.query(
    SamplePeriod.anopheline2_id,
    func.count('*').label('count')
    ).filter(Site.area_type=='point').filter(exists().where(SamplePeriod.site_id==Site.site_id)).filter(Anopheline.id==SamplePeriod.anopheline2_id)

q = session.query(Anopheline.name,
    func.coalesce(sampleperiod_subq.c.site_count,0),
    func.coalesce(sampleperiod_subq.c.sampleperiod_count, 0)
    ).order_by(Anopheline.name.desc())

#reports.append(
#    ExcelReport(
#        'Sundaicus',
#        """
#        select ss1.source_id, ss2.site_id, ss2.km
#        from
#        (
#        select distinct site_id, source_id from  
#        vector_sampleperiod where anopheline2_id = 52
#        ) 
#        as ss1,

#        (
#        select st_distance(eo.geom, pac.geom) * 111.1 as km, site_id
#        from 
#        vector_expertopinion eo, 
#        vector_site_presence_absence_coordinates pac
#        where 
#        eo.anopheline2_id = 52
#        and
#        pac.anopheline2_id = 52
#        ) as ss2
#        where ss1.site_id = ss2.site_id
#        order by 1,2
#        ;
#        """,
#        headers = ["source", "site_id", "distance",]
#    )
#)

reports.append(
    ExcelReport(
        'Sites and sample periods by all species',
        """
        select (select abbreviation from vector_anopheline2 va where va.id = vector_sampleperiod.anopheline2_id),count(distinct(site_id)), count(*) from vector_sampleperiod where anopheline2_id != 8 group by anopheline2_id order by 1
        """,
        headers = ["", "Unique sites", "Temporally unique collections",],
        totals = [1,2]
    )
)

reports.append(
    ExcelReport(
        'Sites and sample periods by 40 DVS',
        """
        SELECT  
        (select name from vector_anopheline2 va where va.id = vs.anopheline2_id), 
        count(distinct(site_id)),  
        count(*)  
        from vector_sampleperiod vs 
        join vector_tagcomment vt 
        on vs.anopheline2_id = vt.anopheline2_id 
        where vt.to_be_mapped 
        group by vs.anopheline2_id order by 1 
        ; 
        """,
        headers = ["", "Unique sites", "Temporally unique collections",],
        totals = [1,2]
    )
)

reports.append(
    ExcelReport(
    'Sites by species and area type all species',
    """
    select
    a.name,
    (select count(*) from site where has_geometry and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as all_sites,
    (select count(*) from site where area_type = 'point' and has_geometry and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as point_count,
    (select count(*) from site where area_type = 'wide area' and has_geometry and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as wide_area,
    (select count(*) from site where area_type = 'polygon small' and has_geometry and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as polygon_small,
    (select count(*) from site where area_type = 'polygon large' and has_geometry and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as polygon_large,
    (select count(*) from site where area_type = 'not specified' and has_geometry and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as not_specified,
    (select count(*) from site where (not has_geometry) and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as all_sites_null_geom,
    (select count(*) from site where area_type = 'point' and (not has_geometry) and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as point_count_null_geom,
    (select count(*) from site where area_type = 'wide area' and (not has_geometry) and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as wide_area_null_geom,
    (select count(*) from site where area_type = 'polygon small' and (not has_geometry) and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as polygon_small_null_geom,
    (select count(*) from site where area_type = 'polygon large' and (not has_geometry) and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as polygon_large_null_geom,
    (select count(*) from site where area_type = 'not specified' and (not has_geometry) and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as not_specified_null_geom
    from vector_anopheline2 a
    order by a.name asc;
    """,
    headers = ["", "All sites", "Points", "Wide area", "Polygon small", "Polygon large", "Not specified", "All sites", "Points", "Wide area", "Polygon small", "Polygon large", "Not specified",],
    totals = range(1,13)
    )
)

reports.append(
    ExcelReport(
    'Sites and area type by 40 DVS',
    """
    select
    a.name,
    (select count(*) from site where has_geometry and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id)) as all_sites,
    (select count(*) from site where area_type = 'point' and has_geometry and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as point_count,
    (select count(*) from site where area_type = 'wide area' and has_geometry and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as wide_area,
    (select count(*) from site where area_type = 'polygon small' and has_geometry and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as polygon_small,
    (select count(*) from site where area_type = 'polygon large' and has_geometry and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as polygon_large,
    (select count(*) from site where area_type = 'not specified' and has_geometry and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as not_specified,
    (select count(*) from site where (not has_geometry) and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as all_sites_null_geom,
    (select count(*) from site where area_type = 'point' and (not has_geometry) and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as point_count_null_geom,
    (select count(*) from site where area_type = 'wide area' and (not has_geometry) and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as wide_area_null_geom,
    (select count(*) from site where area_type = 'polygon small' and (not has_geometry) and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as polygon_small_null_geom,
    (select count(*) from site where area_type = 'polygon large' and (not has_geometry) and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as polygon_large_null_geom,
    (select count(*) from site where area_type = 'not specified' and (not has_geometry) and site_id in (select distinct(site_id) from vector_sampleperiod vsp where a.id = vsp.anopheline2_id))as not_specified_null_geom
    from vector_anopheline2 a
    join vector_tagcomment vt
    on vt.anopheline2_id = a.id
    where vt.to_be_mapped
    order by a.name asc;
    """,
    headers = ["", "All sites", "Points", "Wide area", "Polygon small", "Polygon large", "Not specified", "All sites", "Points", "Wide area", "Polygon small", "Polygon large", "Not specified",],
    totals = range(1,13)
    )
)

reports.append(
    ExcelReport(
        'Non matching sample aggregates',
        """
        select source_id, (select full_name from site sss where sss.site_id = vector_sampleperiod.site_id), id from vector_sampleperiod where 
        id in (
        select vsp.id 
        from 
        vector_sampleperiod vsp
        join vector_collection vc
        on (vc.sample_period_id = vsp.id)
        group by vsp.id
        having (sum(coalesce(vc.count, 1)) > 0)
        ) and vector_sampleperiod.sample_aggregate =0
        order by 1,2
        ;
        """,
        headers = ["enl_id", "site_id", "sample_period_id", ],
        )
    )

reports.append(
    ExcelReport(
        'Presences outside eo',
        """
        select distinct va2.abbreviation, source_id from 
        vector_presence_outside_eo vpeo
        join vector_anopheline2 va2
        on va2.id = vpeo.anopheline2_id
        """,
        headers = ["species", "source_id", "site_id",],
    )
)

reports.append(
    ExcelReport(
        'Combinations of species',
        """
select va.abbreviation, vs.abbreviation, va2.abbreviation, ss.complex, count from
(select anopheline_id, anopheline2_id, subspecies_id, complex, count(*) from vector_sampleperiod group by 1,2,3,4) as ss
join vector_anopheline va on ss.anopheline_id = va.id
join vector_subspecies vs on ss.subspecies_id = vs.id
left join vector_anopheline2 va2 on va2.id = ss.anopheline_id
order by 1, 2, 3;
        """,
        headers = ["Species 1", "Species 2", "Anopheline2", "Complex", "Count",],
        ))



reports.append(
    ExcelReport(
        'Sites in sea',
        """
        SELECT site_id, st_distance, site_coordinate_id, source_id, latitude, 
        longitude
        FROM sites_in_sea;
        """,
        headers = ["site_id", "st_distance", "site_coordinate_id", "source_id", "latitude", "longitude",]
    )
)
reports.append(
    ExcelReport(
        'Wrong country',
        """
        SELECT site_id, full_name FROM site where ;
        """,
        headers = ["site_id", "st_distance", "site_coordinate_id", "source_id", "latitude", "longitude",]
    )
)


reports.append(
    ExcelReport(
    'No name but has coords',
    """
    select full_name, site_id from site where exists (select * from site_coordinates sc where sc.site_id = site.site_id) and full_name in (Null, '', ' ', '  ');
    """,
    headers = ["", "All sites", "Points", "Wide area", "Polygon small", "Polygon large", "Not specified", "All sites", "Points", "Wide area", "Polygon small", "Polygon large", "Not specified",],
    totals = range(1,13)
    )
)

#reports.append(
#    ExcelReport(
#    'vector collections with na in all 4',
#    """
#    select source_id, site_id, (select abbreviation from vector_anopheline where id = vector_sampleperiod.anopheline_id) from
#    vector_sampleperiod where id in (select sample_period_id from vector_collection
#    where collection_method_id = 21
#    group by sample_period_id
#    having sum(ordinal) = 10)
#    order by 1,2;
#    """,
#    headers = ["source_id", "site_id", "anopheline",]
#    )
#)

#reports.append(
#    ExcelReport(
#    'vector collections with na and a count',
#    """
#    select source_id, site_id, sample_period_id, (select abbreviation from vector_anopheline where id = vsp.anopheline_id), vc.count from vector_collection vc inner join vector_sampleperiod vsp on vsp.id = vc.sample_period_id where vc.collection_method_id = 21 and vc.count is not null and ordinal <> 1;
#    """,
#    headers = ["source_id", "site_id", "sample_period_id", "anopheline", "count",]
#    )
#)

reports.append(
    ExcelReport(
    'Map text',
    """
    select abbreviation, author, missing_occurrence
    from
    vector_map
    order by 1;
    """,
    headers = ["source_id", "site_id", "anopheline",]
    )
)

reports.append(
    ExcelReport(
    'Sites with no name',
    """
    select full_name, (select min(source_id) from vector_sampleperiod vsp where vsp.site_id = site.site_id), site_id from site where full_name in (Null, '', ' ', '  ') and has_vector_data is true;
    """,
    headers = ["full_name", "source_id", "site_id", ]
    )
)

reports.append(
    ExcelReport(
    'admin0-vs-country',
    """
    select au.name, c.name as country_name, au.country_id, c.pf_endemic from adminunit au, country c where admin_level = '0' and au.country_id = c.id order by 1;
    """,
    headers = ["admin0_name", "country_name", "country_code", "pf_endemic", ]
    )
)

reports.append(
    ExcelReport(
    'Blank countries',
    """
    select full_name, (select min(source_id) from vector_sampleperiod vsp where vsp.site_id = site.site_id), site_id from site where (country in ('', ' ', '  ') or country is null) and has_vector_data is true order by 2;
    """,
    headers = ["full_name", "source_id", "site_id", ]
    )
)

#reports.append(
#    ExcelReport(
#    'Datasheet random source check',
#    """
#    select ss.source_id, s.full_name, va.abbreviation, ss.count
#    from
#    site s,
#    vector_anopheline va,
#    (select source_id, site_id, anopheline_id, count(*) as count from vector_sampleperiod group by 1,2,3) as ss
#    where s.site_id = ss.site_id and va.id = ss.anopheline_id and ss.source_id in
#    (select source_to_check from vector_tempdatasheetloaded) 
#    order by source_id;
#    """,
#    headers = ["source_id", "full_name", "abbreviation",  "count", ]
#    )
#)



reports.append(
    ExcelReport(
    'Not specified sites',
    """
    select full_name, site_id, (select min(source_id) from vector_sampleperiod vsp where vsp.site_id = site.site_id) from site where has_vector_data = True and area_type = 'not specified'
    order by 3;
    """
    ,
    headers = ["full_name", "site_id", "source_id",]
))

#reports.append(
#    ExcelReport(
#    "Rename me",
#    """
#    #FIXME: ??
#    select s.author_main, s.author_initials, s.enl_id, ss1.count
#    from
#        source s,
#        (select vsp.source_id, count(*) as count
#        from vector_sampleperiod vsp,
#        (
#            select s.site_id, count(*) as c
#            from site s, vector_presence vp
#            where (not has_geometry)
#            and s.site_id = vp.site_id
#            group by s.site_id
#        ) as ss
#        where
#        ss.site_id = vp.site_id
#        group by vp.source_id
#        ) as ss1
#        where
#        ss1.source_id = s.enl_id
#    order by count;
#    """,
#    headers = ["", "Unique sites", "Temporally unique collections",] 
#    )
#)

reports.append(
    ExcelReport(
    "Sites missing coordinates",
    """
    select distinct source_id, site.full_name, site.site_id from vector_sampleperiod, site
    where exists (select * from vector_sampleperiod vsp where vsp.site_id = site.site_id) and vector_sampleperiod.site_id in
    (select site_id from site_coordinates group by site_id having count(*) > 1) and area_type = 'point'
    and vector_sampleperiod.site_id = site.site_id
    order by source_id desc
    """
    )
)

reports.append(
    ExcelReport(
    "Bad sequence in ordinals",
"""
select distinct source_id, site_id  
from vector_sampleperiod where site_id in 
(select site_id as sum_ord from site_coordinates
group by site_id
having not (
    (sum(ordinal) = 1 and count(*) = 1) or
    (sum(ordinal) = 3 and count(*) = 2) or
    (sum(ordinal) = 6 and count(*) = 3) or
    (sum(ordinal) = 10 and count(*) = 4) or
    (sum(ordinal) = 15 and count(*) = 5) or
    (sum(ordinal) = 21 and count(*) = 6) or
    (sum(ordinal) = 28 and count(*) = 7) or
    (sum(ordinal) = 36 and count(*) = 8))
)
;
""",
headers = ["source_id", "site_id",] 
))

reports.append(
    ExcelReport(
    "Presences outside EO",
"""
select va2.abbreviation, source_id, site_id
from vector_presence_outside_eo vpeo
join vector_anopheline2 va2
on va2.id = vpeo.anopheline_id
""",
headers = ["species", "source_id", "site_id",] 
))

reports.append(
    #sa.id = 1 refers to extra coords anomaly
    ExcelReport(
    "Extra coords in point and wide area",
"""
select vsp.source_id, vsp.site_id, (select full_name from site sss where sss.site_id = vsp.site_id) 
from vector_sampleperiod vsp where vsp.site_id in (
select sc.site_id from site s, site_coordinates sc 
where s.site_id = sc.site_id 
and s.area_type in ('point', 'wide area')
group by sc.site_id
having count(*) > 1
)
and vsp.site_id not in (select site_id from site_site_anomaly_check sa where sa.siteanomaly_id = 1)
group by 1,2
order by 1,2

;
""",
headers = ["source_id", "site_id", "full_name",] 
))

reports.append(
    ExcelReport(
    "Kenya vectors not summarized",
"""
select
source.enl_id, source.author_main, source.author_initials, source.year, source.report_type,source.published, 
s.site_id, s.full_name, s.admin1_paper, s.admin2_paper, s.admin3_paper, s.admin2_id,
(select latitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 1),
(select longitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 1),
(select latitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 2),
(select longitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 2),
(select latitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 3),
(select longitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 3),
(select latitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 4),
(select longitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 4),
(select latitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 5),
(select longitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 5),
(select latitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 6),
(select longitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 6),
(select latitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 7),
(select longitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 7),
(select latitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 8),
(select longitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 8),
s.latlong_source,
s.bestguess_good,
s.bestguess_rough,
s.vector_site_notes,
s.area_type,
s.rural_urban,
s.forest,
s.rice,
(select abbreviation from vector_anopheline va where va.id = p.anopheline_id),
(select abbreviation from vector_subspecies vs where vs.id = p.subspecies_id),
p.complex,
start_month, 
end_month, 
start_year, 
end_year,
(select (select abbreviation from vector_collectionmethod cm where vc.collection_method_id = cm.id) from vector_collection vc where vc.sample_period_id = sp.id and vc.ordinal=1),
(select count from vector_collection vc where vc.sample_period_id = sp.id and vc.ordinal=1),
(select (select abbreviation from vector_collectionmethod cm where vc.collection_method_id = cm.id) from vector_collection vc where vc.sample_period_id = sp.id and vc.ordinal=2),
(select count from vector_collection vc where vc.sample_period_id = sp.id and vc.ordinal=2),
(select (select abbreviation from vector_collectionmethod cm where vc.collection_method_id = cm.id) from vector_collection vc where vc.sample_period_id = sp.id and vc.ordinal=3),
(select count from vector_collection vc where vc.sample_period_id = sp.id and vc.ordinal=3),
(select (select abbreviation from vector_collectionmethod cm where vc.collection_method_id = cm.id) from vector_collection vc where vc.sample_period_id = sp.id and vc.ordinal=4),
(select count from vector_collection vc where vc.sample_period_id = sp.id and vc.ordinal=4),
(select (select abbreviation from vector_identificationmethod idm where i.identification_method_id = idm.id) from vector_identification i where i.sample_period_id = sp.id and i.ordinal=1),
(select (select abbreviation from vector_identificationmethod idm where i.identification_method_id = idm.id) from vector_identification i where i.sample_period_id = sp.id and i.ordinal=2),
(select (select abbreviation from vector_identificationmethod idm where i.identification_method_id = idm.id) from vector_identification i where i.sample_period_id = sp.id and i.ordinal=3),
(select (select abbreviation from vector_identificationmethod idm where i.identification_method_id = idm.id) from vector_identification i where i.sample_period_id = sp.id and i.ordinal=4),
(select abbreviation from vector_controlmethod vcm where sp.control_type_id = vcm.id),
(select person from vector_adminlog al where sp.id = al.sample_period_id and al.action = '1'),
(select person from vector_adminlog al where sp.id = al.sample_period_id and al.action = '2'),
(select person from vector_adminlog al where sp.id = al.sample_period_id and al.action = '3'),
sp.notes

FROM 
source
INNER JOIN 
vector_presence p on source.enl_id = p.source_id
INNER JOIN 
site s on p.site_id = s.site_id 
LEFT JOIN
vector_sampleperiod sp 
on (sp.vector_presence_id = p.id)

WHERE 
s.site_id in (select site_id from temp_kv2)
ORDER BY source.enl_id, s.site_id
""",
headers = ['ENL_ID', 'AUTHOR', 'Initials', 'YEAR', 'REPORT_TYPE', 'PUBLISHED', 'SITE_ID', 'FULL_NAME', 'ADMIN1_PAPER', 'ADMIN2_PAPER', 'ADMIN3_PAPER', 'ADMIN2_ID', 'LAT', 'LONG', 'LAT2', 'LONG2', 'LAT3', 'LONG3', 'LAT4', 'LONG4', 'LAT5', 'LONG5', 'LAT6', 'LONG6', 'LAT7', 'LONG7', 'LAT8', 'LONG8', 'LATLONG_SOURCE', 'GOOD_GUESS', 'BAD_GUESS', 'SITE_NOTES', 'AREA_TYPE', 'RURAL-URBAN', 'FOREST', 'RICE', 'SPECIES1', 'SPECIES2', 'COMPLEX', 'MONTH_STVEC', 'MONTH_ENVEC', 'YEAR_STVEC', 'YEAR_ENVEC', 'ID1', 'ID2', 'ID3', 'ID4', 'SAMP_TECH1', 'N1', 'SAMP_TECH2', 'N2', 'SAMP_TECH3', 'N3', 'SAMP_TECH4', 'N4','CONTROL_TYPE', 'DEC_ID', 'DEC_CHECK', 'MAP_CHECK', 'NOTES_VECTOR']
))

reports.append(
    ExcelReport(
    "Kenya vectors",
"""
select
source.enl_id, source.author_main, source.author_initials, source.year, source.report_type,source.published, 
s.site_id, s.full_name, s.admin1_paper, s.admin2_paper, s.admin3_paper, s.admin2_id,
(select latitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 1),
(select longitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 1),
(select latitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 2),
(select longitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 2),
(select latitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 3),
(select longitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 3),
(select latitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 4),
(select longitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 4),
(select latitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 5),
(select longitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 5),
(select latitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 6),
(select longitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 6),
(select latitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 7),
(select longitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 7),
(select latitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 8),
(select longitude from site_coordinates sc where s.site_id = sc.site_id and ordinal = 8),
s.latlong_source,
s.bestguess_good,
s.bestguess_rough,
s.vector_site_notes,
s.area_type,
s.rural_urban,
s.forest,
s.rice,
(select abbreviation from vector_anopheline va where va.id = p.anopheline_id),
(select abbreviation from vector_subspecies vs where vs.id = p.subspecies_id),
p.complex,
date_part('month',sp.min_date) as start_month, 
date_part('month',sp.max_date) as end_month, 
date_part('year',sp.min_date) as start_year, 
date_part('year',sp.max_date) as end_year,
sp.nsamples,
(select abbreviation from vector_identificationmethod vd where vd.id = si1.vim_id),
(select abbreviation from vector_identificationmethod vd where vd.id = si2.vim_id),
(select abbreviation from vector_identificationmethod vd where vd.id = si3.vim_id),
(select abbreviation from vector_identificationmethod vd where vd.id = si4.vim_id),
(select abbreviation from vector_collectionmethod vc where vc.id = smp1.sm_id),
case when pa1.sm_count = 0 then 'Absent' when pa1.sm_count > 0 then 'Present' end,
(select abbreviation from vector_collectionmethod vc where vc.id = smp2.sm_id),
case when pa2.sm_count = 0 then 'Absent' when pa2.sm_count > 0 then 'Present' end,
(select abbreviation from vector_collectionmethod vc where vc.id = smp3.sm_id),
case when pa3.sm_count = 0 then 'Absent' when pa3.sm_count > 0 then 'Present' end,
(select abbreviation from vector_collectionmethod vc where vc.id = smp4.sm_id),
case when pa4.sm_count = 0 then 'Absent' when pa4.sm_count > 0 then 'Present' end,
case when pa_all.sm_count = 0 then 'Absent' when pa_all.sm_count > 0 then 'Present' end
FROM 
source
INNER JOIN 
vector_sampleperiod p on source.enl_id = p.source_id
INNER JOIN 
site s on p.site_id = s.site_id 
LEFT JOIN
(select min(date_from_month_year(start_month, start_year)) as min_date, max(date_from_month_year(end_month, end_year)) as max_date, count(*) as nsamples, site_id, vector_presence_id from vector_sampleperiod group by site_id, vector_presence_id) as sp
on (sp.site_id = s.site_id and p.id = sp.vector_presence_id)
LEFT JOIN
(select site_id, min(identification_method_id) as vim_id, vector_presence_id from vector_sampleperiod vsp inner join vector_identification vi on vi.sample_period_id = vsp.id where vi.ordinal=1 group by site_id, vector_presence_id, identification_method_id) as si1
on (si1.site_id = s.site_id and p.id = si1.vector_presence_id)
LEFT JOIN
(select site_id, min(identification_method_id) as vim_id, vector_presence_id from vector_sampleperiod vsp inner join vector_identification vi on vi.sample_period_id = vsp.id where vi.ordinal=2 group by site_id, vector_presence_id, identification_method_id) as si2
on (si2.site_id = s.site_id and p.id = si2.vector_presence_id)
LEFT JOIN
(select site_id, min(identification_method_id) as vim_id, vector_presence_id from vector_sampleperiod vsp inner join vector_identification vi on vi.sample_period_id = vsp.id where vi.ordinal=3 group by site_id, vector_presence_id, identification_method_id) as si3
on (si3.site_id = s.site_id and p.id = si3.vector_presence_id)
LEFT JOIN
(select site_id, min(identification_method_id) as vim_id, vector_presence_id from vector_sampleperiod vsp inner join vector_identification vi on vi.sample_period_id = vsp.id where vi.ordinal=4 group by site_id, vector_presence_id, identification_method_id) as si4
on (si4.site_id = s.site_id and p.id = si4.vector_presence_id)
LEFT JOIN
(select site_id, min(collection_method_id) as sm_id, vector_presence_id from vector_sampleperiod vsp inner join vector_collection vc on vc.sample_period_id = vsp.id where vc.ordinal=1 group by site_id, vector_presence_id, collection_method_id) as smp1
on (smp1.site_id = s.site_id and p.id = smp1.vector_presence_id)
LEFT JOIN
(select site_id, min(collection_method_id) as sm_id, vector_presence_id from vector_sampleperiod vsp inner join vector_collection vc on vc.sample_period_id = vsp.id where vc.ordinal=2 group by site_id, vector_presence_id, collection_method_id) as smp2
on (smp2.site_id = s.site_id and p.id = smp2.vector_presence_id)
LEFT JOIN
(select site_id, min(collection_method_id) as sm_id, vector_presence_id from vector_sampleperiod vsp inner join vector_collection vc on vc.sample_period_id = vsp.id where vc.ordinal=3 group by site_id, vector_presence_id, collection_method_id) as smp3
on (smp3.site_id = s.site_id and p.id = smp3.vector_presence_id)
LEFT JOIN
(select site_id, min(collection_method_id) as sm_id, vector_presence_id from vector_sampleperiod vsp inner join vector_collection vc on vc.sample_period_id = vsp.id where vc.ordinal=4 group by site_id, vector_presence_id, collection_method_id) as smp4
on (smp4.site_id = s.site_id and p.id = smp4.vector_presence_id)
LEFT JOIN
(select site_id, sum(coalesce(vc.count, 1)) as sm_count, vector_presence_id from vector_sampleperiod vsp inner join vector_collection vc on vc.sample_period_id = vsp.id where vc.ordinal=1 group by site_id, vector_presence_id) as pa1 
on (pa1.site_id = s.site_id and p.id = pa1.vector_presence_id)
LEFT JOIN
(select site_id, sum(coalesce(vc.count, 1)) as sm_count, vector_presence_id from vector_sampleperiod vsp inner join vector_collection vc on vc.sample_period_id = vsp.id where vc.ordinal=2 group by site_id, vector_presence_id) as pa2 
on (pa2.site_id = s.site_id and p.id = pa2.vector_presence_id)
LEFT JOIN
(select site_id, sum(coalesce(vc.count, 1)) as sm_count, vector_presence_id from vector_sampleperiod vsp inner join vector_collection vc on vc.sample_period_id = vsp.id where vc.ordinal=3 group by site_id, vector_presence_id) as pa3 
on (pa3.site_id = s.site_id and p.id = pa3.vector_presence_id)
LEFT JOIN
(select site_id, sum(coalesce(vc.count, 1)) as sm_count, vector_presence_id from vector_sampleperiod vsp inner join vector_collection vc on vc.sample_period_id = vsp.id where vc.ordinal=4 group by site_id, vector_presence_id) as pa4 
on (pa4.site_id = s.site_id and p.id = pa4.vector_presence_id)
LEFT JOIN
(select site_id, sum(coalesce(vc.count, 1)) as sm_count, vector_presence_id from vector_sampleperiod vsp inner join vector_collection vc on vc.sample_period_id = vsp.id group by site_id, vector_presence_id) as pa_all
on (pa_all.site_id = s.site_id and p.id = pa_all.vector_presence_id)
WHERE 
s.site_id in (select site_id from temp_kv2)
ORDER BY source.enl_id, s.site_id
""",
headers = 
['ENL_ID', 'AUTHOR', 'Initials', 'YEAR', 'REPORT_TYPE', 'PUBLISHED', 'SITE_ID', 'FULL_NAME', 'ADMIN1_PAPER', 'ADMIN2_PAPER', 'ADMIN3_PAPER', 'ADMIN2_ID', 'LAT', 'LONG', 'LAT2', 'LONG2', 'LAT3', 'LONG3', 'LAT4', 'LONG4', 'LAT5', 'LONG5', 'LAT6', 'LONG6', 'LAT7', 'LONG7', 'LAT8', 'LONG8', 'LATLONG_SOURCE', 'GOOD_GUESS', 'BAD_GUESS', 'SITE_NOTES', 'AREA_TYPE', 'RURAL-URBAN', 'FOREST', 'RICE', 'SPECIES1', 'SPECIES2', 'COMPLEX', 'MONTH_STVEC', 'MONTH_ENVEC', 'YEAR_STVEC', 'YEAR_ENVEC', 'N_SAMPLE_PERIODS', 'ID1', 'ID2', 'ID3', 'ID4', 'SAMP_TECH1', 'PRESENT/ABSENT', 'SAMP_TECH2', 'PRESENT/ABSENT', 'SAMP_TECH3', 'PRESENT/ABSENT', 'SAMP_TECH4', 'PRESENT/ABSENT', 'OVERALL_PRESENT/ABSENT']
))
