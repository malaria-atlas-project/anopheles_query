# Copyright (C) 2009  William Temperley
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from sqlalchemy import  Table, Column, Integer, String, Float, MetaData, ForeignKey, Boolean, create_engine
from sqlalchemygeom import Geometry
from sqlalchemy.orm import relation, backref, join, mapper, sessionmaker 
from connection_string import connection_string

engine = create_engine(connection_string, echo=False)
metadata = MetaData()
metadata.bind = engine
Session = sessionmaker(bind=engine)

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base(metadata=metadata)

__all__ = ['Anopheline2', 'Source', 'Site', 'SiteCoordinates', 'ExpertOpinion', 'SamplePeriod', 'SitePresenceAbsenceView', 'SpeciesExtentView', 'SamplePeriodPresenceAbsenceView','Session', 'World', 'Map', 'Collection', 'Identification','CollectionMethod', 'IdentificationMethod', 'Region','TagComment',]

"""
Views are used extensively here:
vector_sampleperiod_presence_absence excludes unreliable records and checks for presence / absence.
"""

class Anopheline2(Base):
    """
    """
    __table__ = Table('vector_anopheline2', metadata, autoload=True)
    def __repr__(self):
        return self.name
    def get_scientific_name(self):
        return self.scientific_name.replace('<em>', '<i>').replace('</em>', '</i>')

class TagComment(Base):
    __table__ = Table('vector_tagcomment', metadata, autoload=True)
    anopheline2 = relation("Anopheline2", backref=backref("tag_comment", uselist=False))
    

class IdentificationMethod(Base):
    __table__ = Table('vector_identificationmethod', metadata, autoload=True)

class CollectionMethod(Base):
    __table__ = Table('vector_collectionmethod', metadata, autoload=True)

class Source(Base):
    __table__ = Table('source', metadata, autoload=True)

class Site(Base):
    """
    Represents a georeferenced site. The geometry returns a multipoint shapely object.
    The sample_periods property returns all sample periods linked to the site, aggregated across all studies.
    """
    __table__ = Table('site', metadata, Column('geom', Geometry(4326)), autoload=True)
    sample_periods = relation("SamplePeriod", backref="sites")

class SitePoint(Base):
    __tablename__ = "site_point"
    site_id = Column(Integer, primary_key=True)
    geom = Column(Geometry(4326))

class SiteCoordinates(Base):
    """
    Represents a georeferenced site. The geometry returns a multipoint shapely object.
    The sample_periods property returns all sample periods linked to the site, aggregated across all studies.
    """
    __table__ = Table('site_coordinates', metadata, Column('geom', Geometry(4326)), autoload=True)
    site = relation("Site", backref="site_coordinates")

class ExpertOpinion(Base):
    __tablename__ = "vector_expertopinion"
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry(4326))
    anopheline2_id = Column(Integer, ForeignKey('vector_anopheline2.id'))
    anopheline2 = relation(Anopheline2, backref="expert_opinion")
    reference = Column(String)

class SamplePeriodView(Base):
    __table__ = Table('vector_sampleperiod_site_presence_absence', metadata, Column('id', Integer(), primary_key=True), autoload=True)

class SamplePeriodPresenceAbsenceView(Base):
    __tablename__ = "vector_sampleperiod_presence_absence"
    id = Column(Integer, primary_key=True)
    site_id = Column(Integer)
    start_year = Column(Integer)
    end_year = Column(Integer)
    start_month = Column(Integer)
    end_month = Column(Integer)
    anopheline2_id = Column(Integer, ForeignKey('vector_anopheline2.id'))
    anopheline2 = relation(Anopheline2, backref="sampleperiod_points")
    is_present = Column(Boolean)
    abbreviation = Column(String)
    source_id = Column(Boolean)

class SitePresenceAbsenceView(Base):
    __table__ = Table('vector_site_presence_absence_coordinates', metadata, Column('site_id', Integer(), primary_key=True), autoload=True)

class SpeciesExtentView(Base):
    __tablename__ = "vector_species_extent"
    minx = Column(Float)
    miny = Column(Float)
    maxx = Column(Float)
    maxy = Column(Float)
    anopheline2_id = Column(Integer, ForeignKey('vector_anopheline2.id'), primary_key=True)
    anopheline2 = relation(Anopheline2, backref=backref("data_extent", uselist=False))

class SamplePeriod(Base):
    """
    Represents a vector sample at a location. May have a specified time period.
    vector_site_sample_period is a view which aggregates samples across studies. 
    """
    #NB - A view 
    __tablename__ = "vector_sampleperiod"
    id = Column(Integer, primary_key=True)
    site_id = Column(Integer, ForeignKey('site.site_id'))
    source_id = Column(Integer, ForeignKey('source.enl_id'))
    complex = Column(String)
    anopheline2_id = Column(Integer, ForeignKey('vector_anopheline2.id'))
    anopheline2 = relation(Anopheline2, backref="sample_period")
    start_month = Column(Integer, nullable=True)
    start_year = Column(Integer, nullable=True)
    end_month = Column(Integer, nullable=True)
    end_year = Column(Integer, nullable=True)
    sample_aggregate = Column(Integer, nullable=True)
    tag_recommended_unreliable = Column(Boolean, nullable=True)

class Identification(Base):
    __table__ = Table('vector_identification', metadata, autoload=True)

class Collection(Base):
    """ 
    A set of mosquitos collected by a specific method 
    """ 
    __tablename__ = "vector_collection"
    id = Column(Integer, primary_key=True)
    ordinal = Column(Integer)
    count = Column(Integer)
    sample_period_id = Column(Integer, ForeignKey('vector_sampleperiod.id'))
    sample_period = relation("SamplePeriod", backref="sample")

class Region(Base):
    __table__ = Table('vector_region', metadata, autoload=True)

    def is_valid(self):
        if self.minx is None or self.miny is None or self.maxx is None or self.maxy is None:
            return False
        if self.minx > self.maxx and self.miny > self.maxy:
            return False
        return True
             
    def update(self, new_region):
        #look out for nulls in update
        if not self.minx:
            self.minx = new_region.minx
        if not self.miny:
            self.miny = new_region.miny
        self.minx = min(self.minx, new_region.minx) or self.minx
        self.miny = min(self.miny, new_region.miny) or self.miny
        self.maxx = max(self.maxx, new_region.maxx)
        self.maxy = max(self.maxy, new_region.maxy)
        
    def expand(self, proportion):
        dX = self.maxx - self.minx
        dY = self.maxy - self.miny
        self.minx = self.minx - (dX * proportion)
        self.maxx = self.maxx + (dX * proportion)
        self.miny = self.miny - (dY * proportion)
        self.maxy = self.maxy + (dY * proportion)

#FIXME: keep data and display separate!
class LayerStyle(Base):
    __tablename__ = "vector_layerstyle"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    fill_colour = Column(String)
    line_colour = Column(String)
    line_width = Column(String)
    opacity = Column(Float)
    def to_rgba(self, key):
        hex = self.__getattribute__(key)
        return float(int(hex[1:3], 16))/255,float(int(hex[3:5], 16))/255,float(int(hex[5:7], 16))/255,self.opacity

class Map(Base):
    __tablename__ = "vector_map"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    anopheline2_id = Column(Integer, ForeignKey('vector_anopheline2.id'))
    anopheline2 = relation("Anopheline2")
    map_text = Column(String)
    larval_habitat = Column(String)
    region_id = Column(Integer, ForeignKey('vector_region.id'))
    region = relation(Region, backref="vector_map")
    def get_extent(self):
        if region:
            return region
        else:
            return self.anopheline2.data_extent

class AnophelineLayer(Base):
    __tablename__ = "vector_anophelinelayer"
    id = Column(Integer, primary_key=True)
    ordinal = Column(Integer)
    map_id = Column(Integer, ForeignKey('vector_map.id'))
    map = relation(Map, backref=backref("anopheline_layers", order_by=ordinal))
    style_id = Column(Integer, ForeignKey('vector_layerstyle.id'))
    style = relation(LayerStyle, backref="anopheline_layer")
    layer_type = Column(String)
    is_presence = Column(Boolean, nullable=True)
    anopheline2_id = Column(Integer, ForeignKey('vector_anopheline2.id'))
    anopheline2 = relation(Anopheline2, backref="anopheline_layer")
    class Meta:
        ordering = ('ordinal',)

class World(Base):
    """
    The world as one big multipolygon
    """
    __tablename__ = "world"
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry(4326))
