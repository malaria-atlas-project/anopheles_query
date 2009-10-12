from sqlalchemy.orm import join
from sqlalchemy.sql import func, exists, and_, not_
from models import Anopheline2, SitePoint, SamplePeriodPresenceAbsenceView as SamplePeriodPA, TagComment
from sqlalchemygeom import *
from shapely.geometry import MultiPolygon


__all__ = ['IncompleteDataError', 'list_species', 'species_query',]

class IncompleteDataError(BaseException):
    pass

def species_query(session, species):
    """
    """
    mozzie = session.query(Anopheline2).filter(Anopheline2.id==species).one()

    #Count of sample_periods where there none of this species were caught
    #BUT some of another were caught
    sample_periods_other_species = session.query(
            SamplePeriodPA.site_id,
            func.count('*').label('count')
        ).filter(
           not_(SamplePeriodPA.anopheline2==mozzie) 
        ).group_by(SamplePeriodPA.site_id).subquery()

    #Count of recorded presences of this species
    sample_periods_gt_zero = session.query(
            SamplePeriodPA.site_id,
            func.count('*').label('count')
        ).filter(
            and_(SamplePeriodPA.anopheline2==mozzie, SamplePeriodPA.is_present)
        ).group_by(SamplePeriodPA.site_id).subquery()

    #Count of recorded zeros of this species
    sample_periods_zero = session.query(
            SamplePeriodPA.site_id,
            func.count('*').label('count')
        ).filter(
            and_(SamplePeriodPA.anopheline2==mozzie, not_(SamplePeriodPA.is_present))
        ).group_by(SamplePeriodPA.site_id).subquery()

    #Checksum - the above 3 should add up to this
    all_sample_periods = session.query(
            SamplePeriodPA.site_id,
            func.count('*').label('count')
        ).group_by(SamplePeriodPA.site_id).subquery()

    #Has been sampled for, zero or not. Probably redundant now.
    any_anopheline = exists().where(SamplePeriodPA.site_id==SitePoint.site_id)

#SQL issued to db here - session queries are lazy.
    sites = session.query(
        SitePoint.geom, 
        sample_periods_gt_zero.c.count, 
        sample_periods_zero.c.count,
        sample_periods_other_species.c.count,
        all_sample_periods.c.count
    ).outerjoin(
        (sample_periods_gt_zero, SitePoint.site_id==sample_periods_gt_zero.c.site_id),
        (sample_periods_zero, SitePoint.site_id==sample_periods_zero.c.site_id),
        (sample_periods_other_species, SitePoint.site_id==sample_periods_other_species.c.site_id),
        (all_sample_periods, SitePoint.site_id==all_sample_periods.c.site_id)
    ).filter(any_anopheline)
    mozzie_site_list = sites.all()

    if len(mozzie.expert_opinion)==0:
        #Return null geometry if no eo
        eo_geom = MultiPolygon()
    else:
        eo_geom = mozzie.expert_opinion[0].geom

    return mozzie_site_list, eo_geom

def list_species(session):
    return [(o.id, o.name) for o in session.query(Anopheline2).join(TagComment).filter(TagComment.to_be_mapped==True)]
