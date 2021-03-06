# coding: utf-8
from sqlalchemy import Column, DateTime, Integer, Text, Boolean, String, ForeignKey, \
    BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


# TODO: using Text instead of String(#) (varchar) cause why not


class Round(Base):
    __tablename__ = 'Round'

    id = Column(Integer, primary_key=True)
    name = Column(Text)
    start = Column(DateTime)
    end = Column(DateTime)


class Call(Base):
    __tablename__ = 'Call'

    id = Column(Integer, primary_key=True)
    round_id = Column(Integer, ForeignKey(Round.id))
    start = Column(DateTime)
    end = Column(DateTime)


class Country(Base):
    __tablename__ = 'Country'

    id = Column(Integer, primary_key=True)
    code = Column(String(2))
    name = Column(Text)


class Discipline(Base):
    __tablename__ = 'Discipline'

    id = Column(Integer, primary_key=True)
    name = Column(Text)


class Category(Base):
    __tablename__ = 'Category'

    id = Column(Integer, primary_key=True)
    name = Column(Text)
    higherName = Column(Text)


class Institution(Base):
    __tablename__ = 'Institution'

    id = Column(Integer, primary_key=True)
    acronym = Column(Text)
    name = Column(Text)
    country_id = Column(Integer, ForeignKey(Country.id))


class InstallationFacility(Base):
    __tablename__ = 'InstallationFacility'

    id = Column(Integer, primary_key=True)
    code = Column(Text)
    category_id = Column(Integer, ForeignKey(Category.id))
    institution_id = Column(Integer, ForeignKey(Institution.id))
    description = Column(Text)


class Output(Base):
    __tablename__ = 'Output'

    # rename to id
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    output_type = Column(Text)
    publication_status = Column(Text)
    authors = Column(Text)
    year = Column(Integer)
    title = Column(Text)
    publisher = Column(Text)
    url = Column(Text)
    volume = Column(Text)
    pages = Column(Text)
    conference = Column(Text)
    degree = Column(Text)


class SpecificDiscipline(Base):
    __tablename__ = 'SpecificDiscipline'

    id = Column(Integer, primary_key=True)
    name = Column(Text)
    discipline_id = Column(Integer, ForeignKey(Discipline.id))


class VisitorProject(Base):
    __tablename__ = 'VisitorProject'

    id = Column(Integer, primary_key=True)
    original_project_id = Column(Integer)
    title = Column(Text)
    objectives = Column(Text)
    achievements = Column(Text)
    user_guid = Column(BigInteger)
    user_age_range = Column(Text)
    length_of_visit = Column(Integer)
    start = Column(DateTime)
    end = Column(DateTime)
    # TODO: fk
    taf_id = Column(Integer)
    home_facilities = Column(Boolean)
    application_state = Column(Text)
    acceptance = Column(Boolean)
    summary = Column(Text)
    new_user = Column(Boolean)
    facility_reasons = Column(Text)
    submission_date = Column(DateTime)
    support_final = Column(Boolean)
    project_discipline = Column(Integer, ForeignKey(Discipline.id))
    project_specific_discipline = Column(Integer, ForeignKey(SpecificDiscipline.id))
    call_submitted = Column(Integer, ForeignKey(Call.id))
    previous_application = Column(Boolean)
    training_requirement = Column(Text)
    # TODO: fk?
    supporter_institution = Column(Text)
    administration_state = Column(Text)
    group_leader = Column(Boolean)
    group_members = Column(Text)
    background = Column(Text)
    reasons = Column(Text)
    expectations = Column(Text)
    outputs = Column(Text)
    # TODO: fk?
    group_leader_institution = Column(Text)
    visit_funded_previously = Column(Boolean)
    gender = Column(Text)
    nationality = Column(Integer, ForeignKey(Country.id))
    researcher_status = Column(Text)
    researcher_discipline1 = Column(Integer, ForeignKey(Discipline.id))
    researcher_discipline2 = Column(Integer, ForeignKey(Discipline.id))
    researcher_discipline3 = Column(Integer, ForeignKey(Discipline.id))
    home_institution_type = Column(Text)
    home_institution_dept = Column(Text)
    # TODO: fk?
    home_institution_name = Column(Text)
    home_institution_town = Column(Text)
    home_institution_country = Column(Integer, ForeignKey(Country.id))
    home_institution_postcode = Column(Text)
    number_of_visits = Column(Integer)
    duration_of_stays = Column(Integer)
    nationality_other = Column(Text)
    remote_user = Column(Text)
    # TODO: there's no data in this column in any of the synth databases, do we need it?
    travel_and_subsistence_reimbursed = Column(Text)
    job_title = Column(Text)


class AccessRequest(Base):
    __tablename__ = 'AccessRequest'

    id = Column(Integer, primary_key=True)
    installation_facility_id = Column(Integer, ForeignKey(InstallationFacility.id))
    days_requested = Column(Integer)
    request_detail = Column(Text)
    visitor_project_id = Column(Integer, ForeignKey(VisitorProject.id))


class EvaluationScore(Base):
    __tablename__ = 'EvaluationScore'

    id = Column(Integer, primary_key=True)
    visitor_project_id = Column(Integer, ForeignKey(VisitorProject.id))
    name = Column(Text)
    count = Column(Integer)
    mean = Column(Float)
    mode = Column(Float)
    sum = Column(Float)
    std_dev = Column(Float)
