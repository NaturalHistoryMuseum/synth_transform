# coding: utf-8
from sqlalchemy import Column, DECIMAL, DateTime, Integer, Text, Boolean, String, ForeignKey
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
    round = Column(Integer, ForeignKey('Round.id'))
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
    disciplineName = Column(Text)


class Evaluation(Base):
    __tablename__ = 'Evaluation'

    # reorder: brought the pk to the top, this column is PK_App_Score_ID btw
    id = Column(Integer, primary_key=True)
    # TODO: fk
    userProjectID = Column(Integer)
    # TODO: fk
    TAFScorerID = Column(Integer)
    methodologyScore = Column(DECIMAL(10, 2))
    researchExcellenceScore = Column(DECIMAL(10, 2))
    supportStatementScore = Column(DECIMAL(10, 2))
    justificationScore = Column(DECIMAL(10, 2))
    expectedGainsScore = Column(DECIMAL(10, 2))
    scientificMeritScore = Column(DECIMAL(10, 2))
    USPComment = Column(Text)
    scoredFlag = Column(Integer)
    societalChallengeScore = Column(DECIMAL(10, 2))


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


class Outputs(Base):
    __tablename__ = 'Outputs'

    # rename to id
    id = Column(Integer, primary_key=True)
    userID = Column(Integer)
    outputType = Column(Text)
    authors = Column(Text)
    year = Column(Integer)
    title = Column(Text)
    publisher = Column(Text)
    URL = Column(Text)
    publicationStatusID = Column(Integer)
    volume = Column(Text)
    pages = Column(Text)
    conference = Column(Text)
    degree = Column(Text)
    updatedDate = Column(Text)
    publicationStatus = Column(Text)


class Person(Base):
    __tablename__ = 'Person'

    # rename: id from User_ID
    id = Column(Integer, primary_key=True)
    role = Column(Text)
    gender = Column(Text)
    nationalityCountryCode = Column(Text)
    researcherStatus = Column(Text)
    discipline1 = Column(Integer)
    discipline2 = Column(Integer)
    discipline3 = Column(Integer)
    homeInstitutionType = Column(Text)
    homeInstitutionDept = Column(Text)
    homeInstitutionName = Column(Text)
    homeInstitutionTown = Column(Text)
    homeInstitutionCountryCode = Column(Text)
    homeInstitutionPostcode = Column(Text)
    numberOfVisits = Column(Integer)
    durationOfStays = Column(Integer)
    nationalityOtherText = Column(Text)


class ProjectOutputLink(Base):
    __tablename__ = 'ProjectOutputLink'

    # rename: id from LinkID
    id = Column(Integer, primary_key=True)
    projectID = Column(Integer)
    outputID = Column(Integer)


class Projects(Base):
    __tablename__ = 'Projects'

    # rename: id from UserProject_ID
    id = Column(Integer, primary_key=True)
    TAFID = Column(Integer)
    userID = Column(Integer)
    userProjectTitle = Column(Text)
    userProjectObjectives = Column(Text)
    userProjectAchievements = Column(Text)
    lengthOfVisit = Column(Integer)
    startDate = Column(DateTime)
    finishDate = Column(DateTime)
    homeFacilities = Column(Boolean)
    applicationState = Column(Text)
    acceptance = Column(Boolean)
    userProjectSummary = Column(Text)
    newUser = Column(Boolean)
    userProjectFacilityReasons = Column(Text)
    submissionDate = Column(DateTime)
    supportFinal = Column(Boolean)
    projectDiscipline = Column(Integer)
    projectSpecificDiscipline = Column(Integer)
    callSubmitted = Column(Text)
    previousApplication = Column(Boolean)


class SpecificDiscipline(Base):
    __tablename__ = 'SpecificDiscipline'

    # rename: id from SpecificDisciplineID
    id = Column(Integer, primary_key=True)
    name = Column(Text)


class TAF(Base):
    __tablename__ = 'TAF'

    # rename: id from TAF_ID
    id = Column(Integer, primary_key=True)
    infrastructureShortName = Column(Text)
    name = Column(Text)
