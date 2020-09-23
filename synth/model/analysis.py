# coding: utf-8
from sqlalchemy import Column, DateTime, Integer, Text, Boolean, String, ForeignKey, \
    BigInteger
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


class Facility(Base):
    __tablename__ = 'Facility'

    # rename to id from Category_ID
    id = Column(Integer, primary_key=True)
    category = Column(Text)


class Installation(Base):
    __tablename__ = 'Installation'

    # rename to id from Facility_ID
    id = Column(Integer, primary_key=True)
    installationShortName = Column(Text)
    installationLongName = Column(Text)
    installationDescription = Column(Text)
    categoryID = Column(Integer)


class Institution(Base):
    __tablename__ = 'Institution'

    # rename to id
    id = Column(Integer, primary_key=True)
    institutionName = Column(Text)
    TAFID = Column(Integer)
    countryCode = Column(Text)
    TAFFamilyMember = Column(Boolean)


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


class TAF(Base):
    __tablename__ = 'TAF'

    # rename: id from TAF_ID
    id = Column(Integer, primary_key=True)
    infrastructureShortName = Column(Text)
    name = Column(Text)


class VisitorProject(Base):
    __tablename__ = 'VisitorProject'

    id = Column(Integer, primary_key=True)
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
    # TODO: enum?
    application_state = Column(Text)
    acceptance = Column(Boolean)
    summary = Column(Text)
    new_user = Column(Boolean)
    facility_reasons = Column(Text)
    # TODO: datetime!
    submission_date = Column(Text)
    support_final = Column(Boolean)
    # TODO: fk?
    project_discipline = Column(Integer)
    # TODO: fk?
    project_specific_discipline = Column(Integer)
    # TODO: datetime?
    call_submitted = Column(Integer, ForeignKey(Call.id))
    previous_application = Column(Boolean)
    training_requirement = Column(Text)
    # TODO: fk?
    supporter_institution = Column(Text)
    # TODO: enum?
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
    # TODO: enum?
    gender = Column(Text)
    nationality = Column(Integer, ForeignKey(Country.id))
    researcher_status = Column(Text)
    researcher_discipline1 = Column(Integer, ForeignKey(Discipline.id))
    researcher_discipline2 = Column(Integer, ForeignKey(Discipline.id))
    researcher_discipline3 = Column(Integer, ForeignKey(Discipline.id))
    home_institution_type = Column(Text)
    home_institution_dept = Column(Text)
    home_institution_name = Column(Text)
    home_institution_town = Column(Text)
    home_institution_country = Column(Integer, ForeignKey(Country.id))
    home_institution_postcode = Column(Text)
    number_of_visits = Column(Integer)
    duration_of_stays = Column(Integer)
    nationality_other = Column(Text)
    remote_user = Column(Text)
    # TODO: boolean?
    travel_and_subsistence_reimbursed = Column(Text)
    job_title = Column(Text)
