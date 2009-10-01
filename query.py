from sqlalchemy.orm import join
from sqlalchemy.sql import func, exists, and_, not_
from models import Anopheline2, SamplePeriodPointView
from sqlalchemygeom import *

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
            SamplePeriod.site_id,
            func.count('*').label('count')
        ).filter(
           not_(SamplePeriod.anopheline==mozzie) 
        ).group_by(SamplePeriod.site_id).subquery()

    #Count of recorded presences of this species
    sample_periods_gt_zero = session.query(
            SamplePeriod.site_id,
            func.count('*').label('count')
        ).filter(
            and_(SamplePeriod.anopheline==mozzie, 
            not_(func.coalesce(SamplePeriod.sample_aggregate_check, 1)==0))
        ).group_by(SamplePeriod.site_id).subquery()

    #Count of recorded zeros of this species
    sample_periods_zero = session.query(
            SamplePeriod.site_id,
            func.count('*').label('count')
        ).filter(
            and_(SamplePeriod.anopheline==mozzie, 
            func.coalesce(SamplePeriod.sample_aggregate_check, 1)==0)
        ).group_by(SamplePeriod.site_id).subquery()

    #Checksum - the above 3 should add up to this
    all_sample_periods = session.query(
            SamplePeriod.site_id,
            func.count('*').label('count')
        ).group_by(SamplePeriod.site_id).subquery()

    #Has been sampled for, zero or not. Probably redundant now.
    any_anopheline = exists().where(SamplePeriod.site_id==Site.site_id)

#SQL issued to db here - session queries are lazy.
    sites = session.query(
        Site.geom, 
        sample_periods_gt_zero.c.count, 
        sample_periods_zero.c.count,
        sample_periods_other_species.c.count,
        all_sample_periods.c.count
    ).outerjoin(
        (sample_periods_gt_zero, Site.site_id==sample_periods_gt_zero.c.site_id),
        (sample_periods_zero, Site.site_id==sample_periods_zero.c.site_id),
        (sample_periods_other_species, Site.site_id==sample_periods_other_species.c.site_id),
        (all_sample_periods, Site.site_id==all_sample_periods.c.site_id)
    ).filter(any_anopheline)
    mozzie_site_list = sites.all()

    if len(mozzie.expert_opinion)==0:
        raise IncompleteDataError

    return mozzie_site_list, mozzie.expert_opinion[0].geom

def list_species(session):
    return [(o.id, o.name) for o in session.query(Anopheline)]
