# coding: utf-8
from sqlalchemy import Column, DECIMAL, DateTime, ForeignKey, Integer, String, Table, Text
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


t_CheckTable = Table(
    'CheckTable', metadata,
    Column('Field_tc', String(255)),
    Column('Value_tc', String(255))
)


class CountryIsoCode(Base):
    __tablename__ = 'Country_Iso_Codes'

    Country_Name = Column(String(255))
    Country_Code = Column(String(255), primary_key=True)


t_NHM_Call = Table(
    'NHM_Call', metadata,
    Column('callID', Integer),
    Column('call', Integer),
    Column('dateOpen', DateTime),
    Column('dateClosed', DateTime)
)


class NHMDiscipline(Base):
    __tablename__ = 'NHM_Disciplines'

    DisciplineName = Column(String(255))
    DisciplineID = Column(Integer, primary_key=True)


class NHMEvaluation(Base):
    __tablename__ = 'NHM_Evaluation'

    Eval_ID = Column(Integer, primary_key=True)
    UserProject_ID = Column(Integer)
    Training_Received = Column(Text)
    Seminars = Column(Text)
    Actual_Outputs = Column(Text)
    Further_Access = Column(TINYINT(1))
    Work_Without_Synthesys = Column(TINYINT(1))
    Work_Without_Synthesys_Reason = Column(String(255))
    Admin_Support_Quality = Column(String(255))
    Admin_Support_Comment = Column(Text)
    Application_Process = Column(String(255))
    Training_Quality = Column(String(255))
    Collections_Quality = Column(String(255))
    Data_Quality = Column(String(255))
    Lab_Quality = Column(String(255))
    Analytical_Quality = Column(String(255))
    Visit_Quality = Column(String(255))
    Comparison_Quality = Column(String(255))
    Further_Access_Other_TAF = Column(TINYINT(1))
    Further_Access_Other_TAF_Names = Column(Text)
    SYNTHESYS_Comment = Column(Text)
    Additional_Feedback_Comment = Column(Text)
    TAF_Host_Dept = Column(String(255))


class NHMFacilityCategory(Base):
    __tablename__ = 'NHM_Facility_Categories'

    Category_ID = Column(Integer, primary_key=True)
    Category = Column(String(255))
    TAF_ID = Column(Integer)
    Display_Order = Column(Integer)
    Help = Column(Text)
    Installation_Short_Name = Column(String(255))


class NHMOutputType(Base):
    __tablename__ = 'NHM_OutputTypes'

    OutputType_ID = Column(Integer, primary_key=True)
    OutputType = Column(String(255))
    EU_OutputType = Column(String(255))


class NHMOutputUserProjectLink(Base):
    __tablename__ = 'NHM_Output_UserProject_Link'

    LinkID = Column(Integer, primary_key=True)
    UserProject_ID = Column(Integer)
    Output_ID = Column(Integer)


class NHMOutput(Base):
    __tablename__ = 'NHM_Outputs'

    Output_ID = Column(Integer, primary_key=True)
    User_ID = Column(Integer)
    OutputType_ID = Column(Integer)
    Authors = Column(String(255))
    Year = Column(Integer)
    Title = Column(String(255))
    Publisher = Column(String(255))
    URL = Column(String(255))
    PublicationStatus_ID = Column(Integer)
    Volume = Column(String(255))
    Pages = Column(String(255))
    Conference = Column(String(255))
    Degree = Column(String(255))
    Institute = Column(String(255))
    TrainingDetail = Column(Text)
    UpdatedDate = Column(String(255))


class NHMPublicationStatu(Base):
    __tablename__ = 'NHM_PublicationStatus'

    PublicationStatus_ID = Column(Integer, primary_key=True)
    PublicationStatus = Column(String(255))


class NHMRole(Base):
    __tablename__ = 'NHM_Role'

    Role_ID = Column(Integer, primary_key=True)
    Role_Name = Column(String(255))


class NHMTAF(Base):
    __tablename__ = 'NHM_TAF'

    TAF_ID = Column(Integer, primary_key=True)
    TAF_Name = Column(String(255))
    TAF_Available = Column(TINYINT(1))


class NHMTAFFamilyInstitution(Base):
    __tablename__ = 'NHM_TAF_Family_Institution'

    Institution_ID = Column(Integer, primary_key=True)
    Institution_Name = Column(String(255))
    TAF_ID = Column(Integer)
    Country_Code = Column(String(255))
    TAF_Family_Member = Column(TINYINT(1))
    Alias_ID = Column(Integer)


class NHMUSP(Base):
    __tablename__ = 'NHM_USP'

    USP_ID = Column(Integer, primary_key=True)
    User_ID = Column(Integer)
    TAF_ID = Column(Integer)
    Call = Column(Integer)


class NHMUserProjectsAdditional(Base):
    __tablename__ = 'NHM_UserProjects_Additional'

    NHM_UserProject_ID = Column(Integer, primary_key=True)
    UserProject_ID = Column(Integer)
    Host_Comment = Column(Text)
    Admin_Comment = Column(Text)
    Panel_Comment = Column(Text)
    Feedback_Comment = Column(Text)
    User_Comment = Column(Text)
    Host_Q_Training = Column(Integer)
    Host_Q_Quality = Column(TINYINT(1))
    Host_Q_Objectives = Column(TINYINT(1))
    Host_Q_Host = Column(TINYINT(1))
    Host_Q_Trainer = Column(Integer)
    Host_Q_Timing = Column(TINYINT(1))
    Host_Q_Collections = Column(TINYINT(1))
    Host_Q_Loan = Column(Integer)
    Host_Q_Feasible = Column(TINYINT(1))
    Host_Q_Future = Column(TINYINT(1))
    Host_Q_SupportValid = Column(TINYINT(1))
    Host_Q_MinDuration = Column(String(255))
    Host_Q_AnotherTAF = Column(TINYINT(1))
    Host_Q_OtherTAFNames = Column(String(255))
    Host_Q_Contact = Column(TINYINT(1))


class InfrastructuresInstallation(Base):
    __tablename__ = 'Infrastructures_Installations'

    Contract_ID = Column(String(255))
    Infrastructure_Short_Name = Column(String(255))
    Installation_ID = Column(String(255))
    Installation_Short_Name = Column(String(255), primary_key=True)
    TAF_ID = Column(ForeignKey('NHM_TAF.TAF_ID'), index=True)
    Installation_Long_Name = Column(String(255))
    Display_Order = Column(Integer)

    NHM_TAF = relationship('NHMTAF')


class NHMSpecificDiscipline(Base):
    __tablename__ = 'NHM_Specific_Disciplines'

    SpecificDisciplineID = Column(Integer, primary_key=True)
    SpecificDisciplineName = Column(String(255))
    DisciplineID = Column(ForeignKey('NHM_Disciplines.DisciplineID'), index=True)

    NHM_Discipline = relationship('NHMDiscipline')


class TListOfUser(Base):
    __tablename__ = 'T_List_of_Users'

    Contract_ID = Column(String(255))
    Periodic_Report_ID = Column(String(255))
    UserProject_Acronym = Column(String(255))
    Gender = Column(String(255))
    User_ID = Column(Integer, primary_key=True)
    Nationality_Country_code = Column(ForeignKey('Country_Iso_Codes.Country_Code'), index=True)
    Researcher_status = Column(String(255))
    Discipline1 = Column(ForeignKey('NHM_Disciplines.DisciplineID'), index=True)
    Discipline2 = Column(ForeignKey('NHM_Disciplines.DisciplineID'), index=True)
    Discipline3 = Column(ForeignKey('NHM_Disciplines.DisciplineID'), index=True)
    Home_Institution_Type = Column(String(255))
    Home_Institution_Dept = Column(String(255))
    Home_Institution_Name = Column(String(255))
    Home_Institution_Town = Column(String(255))
    Home_Institution_Country_code = Column(String(255))
    Home_Institution_Postcode = Column(String(255))
    Group_leader = Column(String(255))
    Remote_user = Column(String(255))
    Number_of_visits = Column(Integer)
    Duration_of_stays = Column(Integer)
    Travel_and_Subsistence_reimbursed = Column(String(255))
    Hear_Synthesys = Column(String(255))
    Hear_Synthesys_Source = Column(String(255))
    Home_Institution_Streetname = Column(String(255))
    jobTitle = Column(String(255))
    Nationality_OtherText = Column(String(255))

    NHM_Discipline = relationship('NHMDiscipline', primaryjoin='TListOfUser.Discipline1 == NHMDiscipline.DisciplineID')
    NHM_Discipline1 = relationship('NHMDiscipline', primaryjoin='TListOfUser.Discipline2 == NHMDiscipline.DisciplineID')
    NHM_Discipline2 = relationship('NHMDiscipline', primaryjoin='TListOfUser.Discipline3 == NHMDiscipline.DisciplineID')
    Country_Iso_Code = relationship('CountryIsoCode')


class NHMUsersAdditional(TListOfUser):
    __tablename__ = 'NHM_Users_Additional'

    User_ID = Column(ForeignKey('T_List_of_Users.User_ID'), primary_key=True)
    CV_Qualification = Column(Text)
    CV_Qualification_Year = Column(DateTime)
    CV_Thesis_Title = Column(Text)
    CV_Research_Interests = Column(Text)
    CV_Employment_History = Column(Text)
    CV_Professional_Honours = Column(Text)
    CV_Professional_Membership = Column(Text)
    CV_Other_Info = Column(Text)
    CV_Publications = Column(Text)
    CV_No_Authored = Column(String(255))
    CV_No_Coauthored = Column(String(255))
    CV_No_Oral = Column(String(255))
    CV_No_Posters = Column(String(255))
    CV_No_Refereed = Column(String(255))


class NHMAuthority(Base):
    __tablename__ = 'NHM_Authority'

    TAF_ID = Column(ForeignKey('NHM_TAF.TAF_ID'), index=True)
    User_ID = Column(ForeignKey('T_List_of_Users.User_ID'), index=True)
    Authority_ID = Column(Integer, primary_key=True)
    Role_ID = Column(ForeignKey('NHM_Role.Role_ID'), index=True)

    NHM_Role = relationship('NHMRole')
    NHM_TAF = relationship('NHMTAF')
    T_List_of_User = relationship('TListOfUser')


class NHMInstallationFacility(Base):
    __tablename__ = 'NHM_Installation_Facilities'

    Installation_Description = Column(Text)
    Installation_Short_Name = Column(ForeignKey('Infrastructures_Installations.Installation_Short_Name'), index=True)
    Facility_ID = Column(Integer, primary_key=True)
    Response_Type = Column(String(255))
    Category_ID = Column(ForeignKey('NHM_Facility_Categories.Category_ID'), index=True)
    Help = Column(Text)
    facility_closed = Column(TINYINT(1))

    NHM_Facility_Category = relationship('NHMFacilityCategory')
    Infrastructures_Installation = relationship('InfrastructuresInstallation')


class TListOfUserProject(Base):
    __tablename__ = 'T_List_of_UserProjects'

    Contract_ID = Column(String(255))
    Periodic_Report_ID = Column(String(255))
    UserProject_Title = Column(String(255))
    UserProject_Objectives = Column(String(1000))
    UserProject_Achievements = Column(Text)
    User_ID = Column(ForeignKey('T_List_of_Users.User_ID'), index=True)
    UserProject_ID = Column(Integer, primary_key=True)
    length_of_visit = Column(Integer)
    start_date = Column(DateTime)
    finish_date = Column(DateTime)
    TAF_ID = Column(Integer)
    TAF_Host_Contacted = Column(TINYINT(1))
    Home_Facilities = Column(TINYINT(1))
    TAF_access_need = Column(Text)
    Training_Requirement = Column(Text)
    Supporter_Institution = Column(String(255))
    Supporter_Position = Column(String(255))
    Support_Statement = Column(Text)
    Support_Requested = Column(TINYINT(1))
    Application_State = Column(String(255))
    Administration_State = Column(String(255))
    Acceptance = Column(TINYINT(1))
    Group_leader = Column(TINYINT(1))
    Group_Members = Column(String(1000))
    UserProject_Summary = Column(Text)
    UserProject_Background = Column(Text)
    UserProject_Reasons = Column(Text)
    UserProject_Expectations = Column(Text)
    UserProject_Outputs = Column(Text)
    New_User = Column(TINYINT(1))
    Visited_Details = Column(Text)
    Visited_Link = Column(Text)
    UserProject_Facility_Reasons = Column(Text)
    Submission_Date = Column(String(255))
    Support_Final = Column(TINYINT(1))
    Additional_TAFS = Column(Text)
    Project_Discipline = Column(Integer)
    Project_Specific_Discipline = Column(Integer)
    TAF_Host_Dept = Column(String(255))
    Call_Submitted = Column(String(255))
    Previous_Application = Column(TINYINT(1))
    Previous_Application_Details = Column(Text)
    Group_Leader_Institution = Column(String(255))
    Visit_Funded_Previously = Column(TINYINT(1))
    Visit_Funded_TAFs = Column(String(255))
    Visit_Funded_Details = Column(Text)

    T_List_of_User = relationship('TListOfUser')


class NHMApplicationScore(Base):
    __tablename__ = 'NHM_Application_Scores'

    UserProject_ID = Column(ForeignKey('T_List_of_UserProjects.UserProject_ID'), index=True)
    TAF_Scorer_ID = Column(ForeignKey('T_List_of_Users.User_ID'), index=True)
    Methodology_Score = Column(DECIMAL(10, 2))
    Research_Excellence_Score = Column(DECIMAL(10, 2))
    Support_Stmt_Score = Column(DECIMAL(10, 2))
    Justification_Score = Column(DECIMAL(10, 2))
    Expected_Gains_Score = Column(DECIMAL(10, 2))
    Scientific_Merit_Score = Column(DECIMAL(10, 2))
    USP_Comment = Column(Text)
    PK_App_Score_ID = Column(Integer, primary_key=True)
    Scored_Flag = Column(Integer)
    Societal_Challenge_Score = Column(DECIMAL(10, 2))

    T_List_of_User = relationship('TListOfUser')
    T_List_of_UserProject = relationship('TListOfUserProject')


class NHMUserFacilityNeed(Base):
    __tablename__ = 'NHM_User_Facility_Needs'

    UserProject_ID = Column(ForeignKey('T_List_of_UserProjects.UserProject_ID'), primary_key=True, nullable=False)
    Facility_ID = Column(ForeignKey('NHM_Installation_Facilities.Facility_ID'), primary_key=True, nullable=False, index=True)
    Request = Column(String(255))

    NHM_Installation_Facility = relationship('NHMInstallationFacility')
    T_List_of_UserProject = relationship('TListOfUserProject')
