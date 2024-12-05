from typing import List, Dict, Set

from sqlalchemy import create_engine

from sqlalchemy import select

from sqlalchemy import ForeignKey,ForeignKeyConstraint
from sqlalchemy import UniqueConstraint, CheckConstraint
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import attribute_keyed_dict

from sqlalchemy import event, DDL

# import sqlalchemy as db
import sqlalchemy_utils as db_utils
# from sqlalchemy.orm import declarative_base, sessionmaker, relationship, backref

from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.associationproxy import AssociationProxy

DATA = "data_aug2024.csv"
SYNTAX = "syntax_aug2024.csv"
FILENAME = "ruslinkers-new4"

# Database model
class Base(DeclarativeBase):
    pass

# EXAMPLES

# Examples can illustrate units, parameter values of units, and parameter values of forms

examples_to_unit_parametervalues = Table(
    "examples_to_unit_parametervalues",
    Base.metadata,
    Column("example_id", ForeignKey("examples.id"), primary_key=True),
    Column("unit_id", primary_key=True),
    Column("parametervalue_id", primary_key=True),
    ForeignKeyConstraint(
        ["unit_id","parametervalue_id"],
        ["units_to_parametervalues.unit_id", "units_to_parametervalues.parametervalue_id"]
    )
)

examples_to_form_parametervalues = Table(
    "examples_to_form_parametervalues",
    Base.metadata,
    Column("example_id", ForeignKey("examples.id"), primary_key=True),
    Column("form_id", primary_key=True),
    Column("parametervalue_id", primary_key=True),
    ForeignKeyConstraint(
        ["form_id","parametervalue_id"],
        ["forms_to_parametervalues.form_id", "forms_to_parametervalues.parametervalue_id"]
    )
)

examples_to_units = Table(
    "examples_to_units",
    Base.metadata,
    Column("example_id", ForeignKey("examples.id"), primary_key=True),
    Column("unit_id", ForeignKey("units.id"), primary_key=True)
)

examples_to_forms = Table(
    "examples_to_forms",
    Base.metadata,
    Column("example_id", ForeignKey("examples.id"), primary_key=True),
    Column("form_id", ForeignKey("forms.id"), primary_key=True)
)

class Example(Base):
    __tablename__ = 'examples'

    id: Mapped[int] = mapped_column(primary_key=True)

    text: Mapped[str]

# SOURCES

# Sources can be related to units and meanings (possibly also examples)

sources_to_units = Table(
    "sources_to_units",
    Base.metadata,
    Column("source_id", ForeignKey("sources.id"), primary_key=True),
    Column("unit_id", ForeignKey("units.id"), primary_key=True)
)

class Source(Base):
    __tablename__ = 'sources'

    id: Mapped[int] = mapped_column(primary_key=True)
    biblio: Mapped[str]
    keyword: Mapped[str] = mapped_column(unique=True)

# SEMANTIC FIELDS

class Semfield(Base):
    __tablename__ = 'semfields'

    id: Mapped[int] = mapped_column(primary_key=True)

    name: Mapped[str]
    keyword: Mapped[str] = mapped_column(unique=True)

    subfields: Mapped[Set["Subfield"]] = relationship(back_populates="semfield")

class Subfield(Base):
    __tablename__ = 'subfields'

    id: Mapped[int] = mapped_column(primary_key=True)

    name: Mapped[str]
    keyword: Mapped[str] = mapped_column(unique=True)

    semfield_id: Mapped[int]  = mapped_column(ForeignKey('semfields.id'))
    semfield: Mapped["Semfield"] = relationship(back_populates="subfields")


units_to_semfields = Table(
    "units_to_semfields",
    Base.metadata,
    Column("unit_id", ForeignKey("units.id"), primary_key=True),
    Column("semfield_id", ForeignKey("semfields.id"), primary_key=True)
)

units_to_subfields = Table(
    "units_to_subfields",
    Base.metadata,
    Column("unit_id", ForeignKey("units.id"), primary_key=True),
    Column("subfield_id", ForeignKey("subfields.id"), primary_key=True)
)

# For additional fields associated with specific dictionaries
meanings_to_semfields = Table(
    "meanings_to_semfields",
    Base.metadata,
    Column("meaning_id", ForeignKey("meanings.id"), primary_key=True),
    Column("semfield_id", ForeignKey("semfields.id"), primary_key=True)
)

units_to_subfields = Table(
    "meanings_to_subfields",
    Base.metadata,
    Column("meaning_id", ForeignKey("units.id"), primary_key=True),
    Column("subfield_id", ForeignKey("subfields.id"), primary_key=True)
)

# class UnitToSemfield(Base):
#     __tablename__ = 'units_to_semfields'

#     unit_to_semfield_id: Mapped[int] = mapped_column(primary_key=True)
    
#     semfield_id = db.Column(db.Integer, db.ForeignKey('semfields.semfield_id'), nullable=False)
#     subfield_id = db.Column(db.Integer, db.ForeignKey('subfields.subfield_id')) # Only if there's a subfield

# COMMENTS

class Comment(Base):
    __tablename__ = 'comments'

    id: Mapped[int] = mapped_column(primary_key=True)

    text: Mapped[str]
    hidden: Mapped[bool] = mapped_column(default=True)

    # unit_id: Mapped[int] = mapped_column(ForeignKey("units.id"))
    # unit: Mapped["Unit"] = relationship(back_populates='comments')

comments_to_units = Table(
    "comments_to_units",
    Base.metadata,
    Column("comment_id", ForeignKey("comments.id"), primary_key=True),
    Column("unit_id", ForeignKey("units.id"), primary_key=True)
)

comments_to_unit_parametervalues = Table(
    "comments_to_unit_parametervalues",
    Base.metadata,
    Column("comment_id", ForeignKey("comments.id"), primary_key=True),
    Column("unit_id", primary_key=True),
    Column("parametervalue_id", primary_key=True),
    ForeignKeyConstraint(
        ["unit_id","parametervalue_id"],
        ["units_to_parametervalues.unit_id", "units_to_parametervalues.parametervalue_id"]
    )
)

comments_to_form_parametervalues = Table(
    "comments_to_form_parametervalues",
    Base.metadata,
    Column("comment_id", ForeignKey("comments.id"), primary_key=True),
    Column("form_id", primary_key=True),
    Column("parametervalue_id", primary_key=True),
    ForeignKeyConstraint(
        ["form_id","parametervalue_id"],
        ["forms_to_parametervalues.form_id", "forms_to_parametervalues.parametervalue_id"]
    )
)

# PARAMETERS

class Parameter(Base):
    __tablename__ = "parameters"

    Unit = 1
    Form = 2
    
    id: Mapped[int] = mapped_column(primary_key=True)

    name: Mapped[str]
    keyword: Mapped[str] = mapped_column(unique=True)
    description: Mapped[str] = mapped_column(default = "INSERT TEXT HERE")

    hidden: Mapped[bool] = mapped_column(default=False)
    singleval: Mapped[bool] = mapped_column(default=True) # If parameter can have only one value
    semantic: Mapped[bool] = mapped_column(default=False) # If semantic, otherwise syntactic
    target: Mapped[str] = mapped_column(CheckConstraint("target = 1 OR target = 2"), default=1) # 1 = Unit, 2 = Form

    values: Mapped[Set["ParameterValue"]] = relationship(back_populates='parameter',
                                                         cascade='all,delete-orphan')

class ParameterValue(Base): # Individual values a parameter can take
    __tablename__ = "parametervalues"

    id: Mapped[int] = mapped_column(primary_key=True)

    name: Mapped[str]
    keyword: Mapped[str]
    description: Mapped[str] = mapped_column(default = "INSERT TEXT HERE")    

    parameter_id: Mapped[int] = mapped_column(ForeignKey("parameters.id"))
    parameter: Mapped["Parameter"] = relationship(back_populates='values')

    __table_args__ = (UniqueConstraint('keyword', 'parameter_id'),
                     ) # Ensure that each parameter value is unique within the scope of one parameter

class TextParameter(Base): # Parameters whose values are free-form text
    __tablename__ = 'textparameters'

    Unit = 1
    Form = 2

    id: Mapped[int] = mapped_column(primary_key=True)

    name: Mapped[str]
    keyword: Mapped[str] = mapped_column(unique=True)
    description: Mapped[str] = mapped_column(default = "INSERT TEXT HERE")

    hidden: Mapped[bool] = mapped_column(default=False)
    target: Mapped[str] = mapped_column(CheckConstraint("target = 1 OR target = 2"), default=1)

# Parameter mappings

class UnitToParameterValue(Base):
    __tablename__ = 'units_to_parametervalues' # Units are mapped to parameter values

    unit_id: Mapped[int] = mapped_column(ForeignKey('units.id'), primary_key=True)
    unit: Mapped["Unit"] = relationship(back_populates="parametervalue_mappings")

    parametervalue_id: Mapped[int] = mapped_column(ForeignKey('parametervalues.id'), primary_key=True)
    parametervalue: Mapped["ParameterValue"] = relationship()
    
    parameter: AssociationProxy["Parameter"] = association_proxy("parametervalue", "parameter")
    # parameter_kw: AssociationProxy["ParameterValue"] = association_proxy("parametervalue", "parameter_kw")

    examples: Mapped[Set["Example"]] = relationship(secondary=examples_to_unit_parametervalues)
    comments: Mapped[Set["Comment"]] = relationship(secondary=comments_to_unit_parametervalues,
                                                    cascade='all,delete-orphan')
    # examples = relationship('Example', backref='param') Make a separate linking table

class FormToParameterValue(Base):
    __tablename__ = 'forms_to_parametervalues' # mainly for correlatives, but perhaps also for others

    form_id: Mapped[int] = mapped_column(ForeignKey('forms.id'), primary_key=True)
    form: Mapped["Form"] = relationship(back_populates='parametervalue_mappings')

    parametervalue_id: Mapped[int] = mapped_column(ForeignKey('parametervalues.id'), primary_key=True) # Maybe add constraints that ensure that correct parameters are chosen
    parametervalue: Mapped["ParameterValue"] = relationship()

    examples: Mapped[Set["Example"]] = relationship(secondary=examples_to_form_parametervalues)
    comments: Mapped[Set["Comment"]] = relationship(secondary=comments_to_form_parametervalues,
                                                    cascade='all,delete-orphan')

class UnitToTextParameter(Base):
    __tablename__ = 'units_to_textparametervalues' # For text parameters, you just map parameters to text values

    unit_id: Mapped[int] = mapped_column(ForeignKey('units.id'), primary_key=True)
    unit: Mapped["Unit"] = relationship(back_populates="textparametervalues")

    parameter_id: Mapped[int] = mapped_column(ForeignKey('textparameters.id'), primary_key=True)
    parameter: Mapped["TextParameter"] = relationship()

    value: Mapped[str]

class FormToTextParameter(Base):
    __tablename__ = 'forms_to_textparametervalues' # For text parameters, you just map parameters to text values

    form_id: Mapped[int] = mapped_column(ForeignKey('forms.id'), primary_key=True)
    form: Mapped["Form"] = relationship(back_populates="textparametervalues")

    parameter_id: Mapped[int] = mapped_column(ForeignKey('textparameters.id'), primary_key=True)
    parameter: Mapped["TextParameter"] = relationship()

    value: Mapped[str]    

# UNITS

class Unit(Base):
    __tablename__ = 'units'

    id: Mapped[int] = mapped_column(primary_key=True)
    linker: Mapped[str] # Head word (not treated as Form)

    #internal_id = db.Column(db.Integer)
    status: Mapped[bool] = mapped_column(default=True)  # will be found in dictionary search (1) or not (?)

    # Hardcoded parameters
    style: Mapped[str] = mapped_column(nullable=True)
    sem_comment: Mapped[str] = mapped_column(nullable=True)

    # Connections between units
    links: Mapped[Set["UnitToUnit"]] = relationship(back_populates="source", foreign_keys='UnitToUnit.source_id')

    # Semantic fields
    semfield_id: Mapped[int] = mapped_column(ForeignKey("semfields.id"))
    semfield: Mapped['Semfield'] = relationship()
    extra_semfields: Mapped[Set["Semfield"]] = relationship(secondary=units_to_semfields)
    subfields: Mapped[Set["Subfield"]] = relationship(secondary=units_to_subfields) # Maybe somehow check that subfields belong to the semfields (main and extra)?

    forms: Mapped[Set["Form"]] = relationship(back_populates='unit',cascade='all,delete-orphan')
    meanings: Mapped[Set["Meaning"]] = relationship(back_populates='unit',cascade='all,delete-orphan')
    # log = relationship('Entry_logs', backref='unit', lazy=True)
    # comments = db.relationship('Unit_comments', backref='unit', lazy=True)
    # pictures = db.relationship('Unit_pictures', backref='unit', lazy=True)
    parametervalue_mappings: Mapped[Set["UnitToParameterValue"]] = relationship(back_populates='unit',
                                                                                cascade='all,delete-orphan')
    parametervalues: AssociationProxy[Set["ParameterValue"]] = association_proxy(
        "parametervalue_mappings",
        "parametervalue",
        creator=lambda param: UnitToParameterValue(parametervalue = param)
        )
    parameters: AssociationProxy[Set["Parameter"]] = association_proxy("parametervalue_mappings", "parameter")

    textparametervalues: Mapped[Set["UnitToTextParameter"]] = relationship(back_populates='unit',
                                                                           cascade='all,delete-orphan')
    textparameters: AssociationProxy[Set["TextParameter"]] = association_proxy("textparametervalues", "parameter")

    def get_values_for_parameter(self, param: Parameter) -> List[ParameterValue]:
        if param.target is not Parameter.Unit:
            raise ValueError("Parameter %s does not classify units" % param.keyword)
        return [x for x in param.values if x in self.parametervalues]    

    comments: Mapped[Set["Comment"]] = relationship(secondary=comments_to_units,
                                                    cascade='all,delete-orphan')
    examples: Mapped[Set["Example"]] = relationship(secondary=examples_to_units)
    sources: Mapped[Set["Source"]] = relationship(secondary=sources_to_units)

    # __table_args__ = (UniqueConstraint('linker', 'semfield_id'),
    #                  ) # Ensures that the combination of linker and semantic field is unique

class UnitLinkType(Base):
    __tablename__ = 'unitlinktypes'

    id: Mapped[int] = mapped_column(primary_key=True)
    
    name: Mapped[str]
    keyword: Mapped[str] = mapped_column(unique=True)

class UnitToUnit(Base): #connections between units
    __tablename__ = 'units_to_units'

    # id: Mapped[int] = mapped_column(primary_key=True)

    # rank = db.Column(db.Integer, nullable=True)

    source_id: Mapped[int] = mapped_column(ForeignKey('units.id'), primary_key=True)
    target_id: Mapped[int] = mapped_column(ForeignKey('units.id'), primary_key=True)
    unitlinktype_id: Mapped[int] = mapped_column(ForeignKey('unitlinktypes.id'), primary_key=True)

    source: Mapped["Unit"] = relationship(foreign_keys=[source_id], back_populates="links")
    target: Mapped["Unit"] = relationship(foreign_keys=[target_id])
    unitlinktype: Mapped["UnitLinkType"] = relationship()

# class Label(Base):
#     __tablename__ = 'labels' # Assign a parameter value to a Unit

#     label_id = db.Column(db.Integer, primary_key=True)
#     label = db.Column(db.Text, unique=True)
    # decode = db.Column(db.Text)
    # rank = db.Column(db.Integer, unique=True)
    # label_type = db.Column(db.Integer, unique=False) # what column is this label from? 1 -- number of components, 2 -- position, 3 -- ...        

# FORMS

class Form(Base):
    __tablename__ = 'forms'

    id: Mapped[int] = mapped_column(primary_key=True)

    unit_id: Mapped[int] = mapped_column(ForeignKey('units.id'))
    unit: Mapped["Unit"] = relationship(back_populates="forms")

    formtype_id: Mapped[int] = mapped_column(ForeignKey('formtypes.id'))
    formtype: Mapped["FormType"] = relationship(back_populates="forms")

    # gloss_id = db.Column(db.Integer, db.ForeignKey('glosses.gloss_id'), nullable=False)
    text: Mapped[str]

    parametervalue_mappings: Mapped[Set["FormToParameterValue"]] = relationship(back_populates='form',
                                                                                cascade='all,delete-orphan')
    parametervalues: AssociationProxy[Set["ParameterValue"]] = association_proxy(
        "parametervalue_mappings", 
        "parametervalue", 
        creator = lambda param: FormToParameterValue(parametervalue = param)
        )
    parameters: AssociationProxy[Set["Parameter"]] = association_proxy("parametervalue_mappings", "parameter")

    textparametervalues: Mapped[Set["FormToTextParameter"]] = relationship(back_populates='form',
                                                                           cascade='all,delete-orphan')
    textparameters: AssociationProxy[Set["TextParameter"]] = association_proxy("textparametervalues", "parameter")

    examples: Mapped[Set["Example"]] = relationship(secondary=examples_to_forms)

    # __table_args__ = (UniqueConstraint('unit_id', 'formtype_id', "text"),
    #                  ) # Only one unit-type mapping for a given text value

    def get_values_for_parameter(self, param: Parameter) -> List[ParameterValue]:
        if param.target is not Parameter.Form:
            raise ValueError("Parameter %s does not classify forms" % param.keyword)
        return [x for x in param.values if x in self.parametervalues]

parameters_to_formtypes = Table(
    "parameters_to_formtypes",
    Base.metadata,
    Column("parameter_id", ForeignKey("parameters.id")),
    Column("formtype_id", ForeignKey("formtypes.id"))
)

textparameters_to_formtypes = Table(
    "textparameters_to_formtypes",
    Base.metadata,
    Column("textparameter_id", ForeignKey("parameters.id")),
    Column("formtype_id", ForeignKey("formtypes.id"))
)

class FormType(Base):
    __tablename__ = 'formtypes' # linker, correl, phonvar, mainpart

    id: Mapped[int] = mapped_column(primary_key=True)
    
    name: Mapped[str]
    keyword: Mapped[str] = mapped_column(unique=True)

    forms: Mapped[Set["Form"]] = relationship(back_populates="formtype")
    parameters: Mapped[Set["Parameter"]] = relationship(secondary=parameters_to_formtypes)

# MEANINGS

class Meaning(Base):
    __tablename__ = 'meanings'

    id: Mapped[int] = mapped_column(primary_key=True)

    meaning: Mapped[str]
    pos: Mapped[str]
    pos_type: Mapped[str]
    other_senses: Mapped[str]
    other_pos: Mapped[str]

    unit_id: Mapped[int] = mapped_column(ForeignKey('units.id'))
    unit: Mapped["Unit"] = relationship()

    source_id: Mapped[int] = mapped_column(ForeignKey('sources.id'))
    source: Mapped["Source"] = relationship()

# Various additional triggers for constraints that cannot be handled via UNIQUE, CHECK etc.

# Ensures that single-valued parameters cannot be assigned more than one value for a unit
event.listen(UnitToParameterValue.__table__, 'after_create', DDL('''\
CREATE TRIGGER TR_units_to_parametervalues_INSERT_singleval
	AFTER INSERT
	ON units_to_parametervalues
	WHEN (SELECT p.singleval FROM parameters AS p WHERE p.id = (SELECT pv.parameter_id FROM parametervalues AS pv WHERE pv.id = NEW.parametervalue_id)) = 1 AND
		 (SELECT COUNT(*) from units_to_parametervalues
			WHERE 	unit_id = NEW.unit_id AND
					parametervalue_id IN (SELECT id FROM parametervalues 
											WHERE parameter_id = (SELECT parameter_id FROM parametervalues WHERE id = NEW.parametervalue_id))) > 1
BEGIN
	SELECT RAISE(ABORT, 'ERROR: More than one value assigned to a single-valued unit parameter.');
END;'''))
event.listen(UnitToParameterValue.__table__, 'after_create', DDL('''\
CREATE TRIGGER TR_units_to_parametervalues_UPDATE_singleval
	AFTER INSERT
	ON units_to_parametervalues
	WHEN (SELECT p.singleval FROM parameters AS p WHERE p.id = (SELECT pv.parameter_id FROM parametervalues AS pv WHERE pv.id = NEW.parametervalue_id)) = 1 AND
		 (SELECT COUNT(*) from units_to_parametervalues
			WHERE 	unit_id = NEW.unit_id AND
					parametervalue_id IN (SELECT id FROM parametervalues 
											WHERE parameter_id = (SELECT parameter_id FROM parametervalues WHERE id = NEW.parametervalue_id))) > 1
BEGIN
	SELECT RAISE(ABORT, 'ERROR: More than one value assigned to a single-valued unit parameter.');
END;'''))

# Ensures that form parameters cannot be assigned to units
event.listen(UnitToParameterValue.__table__, 'after_create', DDL('''\
CREATE TRIGGER TR_units_to_parametervalues_INSERT_unitval
	AFTER INSERT
	ON units_to_parametervalues
	WHEN EXISTS (SELECT 1 FROM parametervalues AS pv
					INNER JOIN parameters AS p
					ON pv.parameter_id = p.id
					WHERE pv.id = NEW.parametervalue_id AND p.target = 2)
BEGIN
	SELECT RAISE(ABORT, 'ERROR: Attempt to assign Form parameter to Unit.');
END;'''))
event.listen(UnitToParameterValue.__table__, 'after_create', DDL('''\
CREATE TRIGGER TR_units_to_parametervalues_UPDATE_unitval
	AFTER UPDATE
	ON units_to_parametervalues
	WHEN EXISTS (SELECT 1 FROM parametervalues AS pv
					INNER JOIN parameters AS p
					ON pv.parameter_id = p.id
					WHERE pv.id = NEW.parametervalue_id AND p.target = 2)
BEGIN
	SELECT RAISE(ABORT, 'ERROR: Attempt to assign Form parameter to Unit.');
END;'''))

# Same stuff for form parameters
event.listen(FormToParameterValue.__table__, 'after_create', DDL('''\
CREATE TRIGGER TR_forms_to_parametervalues_INSERT_singleval
	AFTER INSERT
	ON forms_to_parametervalues
	WHEN (SELECT p.singleval FROM parameters AS p WHERE p.id = (SELECT pv.parameter_id FROM parametervalues AS pv WHERE pv.id = NEW.parametervalue_id)) = 1 AND
		 (SELECT COUNT(*) from forms_to_parametervalues
			WHERE 	form_id = NEW.form_id AND
					parametervalue_id IN (SELECT id FROM parametervalues 
											WHERE parameter_id = (SELECT parameter_id FROM parametervalues WHERE id = NEW.parametervalue_id))) > 1
BEGIN
	SELECT RAISE(ABORT, 'ERROR: More than one value assigned to a single-valued form parameter.');
END;'''))
event.listen(FormToParameterValue.__table__, 'after_create', DDL('''\
CREATE TRIGGER TR_forms_to_parametervalues_UPDATE_singleval
	AFTER INSERT
	ON forms_to_parametervalues
	WHEN (SELECT p.singleval FROM parameters AS p WHERE p.id = (SELECT pv.parameter_id FROM parametervalues AS pv WHERE pv.id = NEW.parametervalue_id)) = 1 AND
		 (SELECT COUNT(*) from forms_to_parametervalues
			WHERE 	form_id = NEW.form_id AND
					parametervalue_id IN (SELECT id FROM parametervalues 
											WHERE parameter_id = (SELECT parameter_id FROM parametervalues WHERE id = NEW.parametervalue_id))) > 1
BEGIN
	SELECT RAISE(ABORT, 'ERROR: More than one value assigned to a single-valued form parameter.');
END;'''))
event.listen(FormToParameterValue.__table__, 'after_create', DDL('''\
CREATE TRIGGER TR_forms_to_parametervalues_INSERT_formval
	AFTER INSERT
	ON forms_to_parametervalues
	WHEN EXISTS (SELECT 1 FROM parametervalues AS pv
					INNER JOIN parameters AS p
					ON pv.parameter_id = p.id
					WHERE pv.id = NEW.parametervalue_id AND p.target = 1)
BEGIN
	SELECT RAISE(ABORT, 'ERROR: Attempt to assign Unit parameter to Form.');
END;'''))
event.listen(FormToParameterValue.__table__, 'after_create', DDL('''\
CREATE TRIGGER TR_forms_to_parametervalues_UPDATE_formval
	AFTER UPDATE
	ON forms_to_parametervalues
	WHEN EXISTS (SELECT 1 FROM parametervalues AS pv
					INNER JOIN parameters AS p
					ON pv.parameter_id = p.id
					WHERE pv.id = NEW.parametervalue_id AND p.target = 1)
BEGIN
	SELECT RAISE(ABORT, 'ERROR: Attempt to assign Unit parameter to Form.');
END;'''))

# Ensure that form parameters are assigned to correct form types
event.listen(FormToParameterValue.__table__, 'after_create', DDL('''\
CREATE TRIGGER TR_forms_to_parametervalues_INSERT_formtype
	AFTER INSERT
	ON forms_to_parametervalues
	WHEN NOT EXISTS (SELECT 1 FROM parametervalues AS pv
				INNER JOIN parameters_to_formtypes AS pft
					ON pft.parameter_id = pv.parameter_id
				INNER JOIN forms AS f
					ON f.formtype_id = pft.formtype_id
				WHERE f.id = NEW.form_id AND pv.id = NEW.parametervalue_id)
BEGIN
	SELECT RAISE(ABORT, 'ERROR: Incorrect parameter value for form type.');
END;'''))
event.listen(FormToParameterValue.__table__, 'after_create', DDL('''\
CREATE TRIGGER TR_forms_to_parametervalues_UPDATE_formtype
	AFTER UPDATE
	ON forms_to_parametervalues
	WHEN NOT EXISTS (SELECT 1 FROM parametervalues AS pv
				INNER JOIN parameters_to_formtypes AS pft
					ON pft.parameter_id = pv.parameter_id
				INNER JOIN forms AS f
					ON f.formtype_id = pft.formtype_id
				WHERE f.id = NEW.form_id AND pv.id = NEW.parametervalue_id)
BEGIN
	SELECT RAISE(ABORT, 'ERROR: Incorrect parameter value for form type.');
END;'''))

# Begin script

import csv

# Create SQLite database engine
engine = create_engine('sqlite:///%s.db' % FILENAME)
conn = engine.connect()

# Create file if does not exist
# if not db_utils.database_exists(engine.url):
db_utils.create_database(engine.url)

# Create the tables
Base.metadata.create_all(engine)

# Create the session object
Session = sessionmaker(bind=engine)
session = Session()

with open(SYNTAX) as file:
    reader = csv.DictReader(file, delimiter=',')
    syntax = list(reader)

with open(DATA) as file:
    reader = csv.DictReader(file, delimiter=',')
    data = list(reader)

# Create the parameters

semfields_dict = { }
subfields_dict = { }

for row in syntax:
    semfield_kw = row["semfield1_ed"]
    if semfield_kw == '' or semfield_kw == 'NA':
        print("WARNING: linker %s in SYNTAX has no semantic field!" % row["linker"])
        continue
    if semfield_kw not in semfields_dict.keys():
        semfield = Semfield(
            name = semfield_kw,
            keyword = semfield_kw
        )
        session.add(semfield)
        semfields_dict[semfield_kw] = semfield
    for subfield_kw in row["subfield1_ed"].split("; "):
        if subfield_kw != '' and subfield_kw != 'NA' and subfield_kw not in subfields_dict.keys():
            subfield = Subfield(
                    name = subfield_kw,
                    keyword = subfield_kw,
                    semfield = semfields_dict[semfield_kw]
                )
            session.add(subfield)
            subfields_dict[subfield_kw] = subfield

dummy_field = Semfield(
    name = "EMPTY SEMFIELD",
    keyword = "dummy"
) 

sources = set([x["dict"] for x in data])
sources.add("ИМК")
sources_dict = { }

for sourcename in sources:
    if sourcename != '':
        source = Source(
            biblio = sourcename,
            keyword = sourcename
        )
        session.add(source)
        sources_dict[sourcename] = source

# Form types

# type_linker = FormType(
#     name = "коннектор",
#     keyword = "linker"
# )
# session.add(type_linker)

type_correl = FormType(
    name = "коррелят",
    keyword = "correl"
)
session.add(type_correl)

type_phonvar = FormType(
    name = "фонетический вариант",
    keyword = "phonvar"
)
session.add(type_phonvar)

type_mainpart = FormType(
    name = "основной компонент",
    keyword = "mainpart"
)
session.add(type_mainpart)

# type_component = FormType(
#     name= "второстепенный компонент",
#     keyword = "comp"
# )
# session.add(type_component)

# comp_oblig = Parameter(
#     name='обязательность компонента',
#     keyword='comp.oblig',
#     target=Parameter.Form
#     )
# comp_oblig_yes = ParameterValue(
#         parameter = comp_oblig,
#         keyword="obligatory",
#         name="обязательный"
#     )

# comp_oblig_no = ParameterValue(
#         parameter = comp_oblig,
#         keyword="optional",
#         name="необязательный"
#     )

# type_component.parameters.append(comp_oblig)


# for row in syntax:
#     unit = Unit()
#     session.add(unit)
#     linker = Form(
#         text = row["linker"],
#         formtype = type_linker,
#         unit = unit
#     )
#     session.add(linker)

def process_parameter(full_name, column_name, table, hidden = False, singleval = True, target = Parameter.Unit):
    param = Parameter(
        name = full_name,
        keyword = column_name,
        hidden = hidden,
        singleval = singleval,
        target = target
    )
    session.add(param)

    vals = set([x[column_name] for x in table])

    uniquevals = set()

    valdict = { }

    for val in vals:
        if val != '' and val != 'NA':
            for subval in val.split("; "):
                if subval not in uniquevals:
                    parval = ParameterValue(
                        name = subval,
                        keyword = subval,
                        parameter = param
                    )
                    param.values.add(parval)
                    uniquevals.add(subval)
                    valdict[subval] = parval
    return param, valdict # return a tuple of parameter and dictionary of its values

synt_params = {}
correl_params = {}
# comp_params = {}
synt_params["parts.num"] = process_parameter(
    "количество компонентов", "parts.num", syntax)
synt_params["parts.order"] = process_parameter(
    "порядок компонентов", "parts.order", syntax)
synt_params["linker_position"] = process_parameter(
    "позиция коннектора", "linker_position", syntax, singleval=False)
# synt_params["comp.oblig"] = process_parameter(
#     "обязательность компонента", "comp.oblig", syntax) # These components should be handled as separate Forms with their own obligatoriness parameter!
synt_params["clause.order"] = process_parameter(
    "порядок клауз",
    "clause.order",
    syntax,
    singleval=False
)
synt_params["dep.clause.type"] = process_parameter(
    "тип зависимой клаузы",
    "dep.clause.type",
    syntax,
    singleval=False
)

synt_params["indep.sentence"] = process_parameter(
    "используется в независимом предложении",
    "indep.sentence",
    syntax
)

synt_params["linker_position_exclusivity"] = process_parameter(
    "единственность позиции",
    "linker_position_exclusivity",
    syntax
)

# From alldict

# sem_params = {}

# sem_params["Стилистич. ограничения"] = process_parameter(
#     "стилистические ограничения",
#     "Стилистич. ограничения",
#     data
# )

# TEXT PARAMETERS expansion, comp.oblig

synt_text_params = {}

synt_text_params["expansion"] = TextParameter(
    keyword = "expansion",
    name = "расширение"
)

synt_text_params["comp.oblig"] = TextParameter(
    keyword = "comp.oblig",
    name = "обязательность компонентов"
)
synt_text_params["dep.clause.type"] = TextParameter(
    keyword = "dep.clause.type",
    name = "тип зависимой клаузы"
)
synt_text_params["expansion"] = TextParameter(
    keyword = "expansion",
    name = "возможность расширения"
)
for p in synt_text_params.values(): session.add(p)

# From alldict

# sem_text_params = {}

# sem_text_params["sem_comment"] = TextParameter(
#     keyword="sem_comment",
#     name="комментарий к семантике"
# )

# PARAMETERS WITHOUT COLUMNS

inferential_param = Parameter(
    keyword = 'inferential',
    name = 'инферентивное прочтение',
    semantic = True
)

inferential_param_yes = ParameterValue(
    keyword = "yes",
    name = "возможно",
    parameter = inferential_param
)

inferential_param_no = ParameterValue(
    keyword = "no",
    name = "не засвидетельствовано",
    parameter = inferential_param
)

illoc_param = Parameter(
    keyword = 'illocutionary',
    name = 'иллокутивное прочтение',
    semantic = True
)

illoc_param_yes = ParameterValue(
    keyword = "yes",
    name = "возможно",
    parameter = illoc_param
)

illoc_param_no = ParameterValue(
    keyword = "no",
    name = "не засвидетельствовано",
    parameter = illoc_param
)

metatext_param = Parameter(
    keyword = 'metatextual',
    name = 'метатекстовое прочтение',
    semantic = True
)

metatext_param_yes = ParameterValue(
    keyword = "yes",
    name = "возможно",
    parameter = metatext_param
)

metatext_param_no = ParameterValue(
    keyword = "no",
    name = "не засвидетельствовано",
    parameter = metatext_param
)

# inferential.example
# illoc example
# metatext example - SHOULD BE VIEWED AS PARAMETERS WITH ASSOCIATED EXAMPLES??


correl_params["correl.position"] = process_parameter("позиция коррелята", "correl.position", syntax, singleval=False, target=Parameter.Form)
type_correl.parameters.add(correl_params["correl.position"][0])

# TEXT PARAMETERS FOR CORRELATIVES
correl_text_params = {}
correl_text_params["correl.oblig"] = TextParameter(
    keyword = "correl.oblig",
    name = "обязательность коррелята"
)
# type_correl.parameters.add(correl_params["correl.oblig"][0])

# Fill in the units!

for row in syntax:
    unit = Unit(linker = row["linker"])
    session.add(unit)
    # unit.forms.append(Form(
    #     text = row["linker"],
    #     formtype = type_linker
    # ))
    unit.semfield = semfields_dict[row["semfield1_ed"]]
    for subfield in row["subfield1_ed"].split("; "):
        if subfield != '' and subfield != 'NA':
            unit.subfields.add(subfields_dict[subfield])    
    for source in row["source"].split("; "):
        try: unit.sources.add(sources_dict[source])
        except KeyError: print("WARNING: Entry '%s' on line %d in SYNTAX has no source!" % (row["linker"], syntax.index(row)+2))
    for param in synt_params.keys():
        if row[param] != '' and row[param] != 'NA':
            for parval in row[param].split('; '):
                if parval != '' and parval != 'NA':
                    parvalmap = UnitToParameterValue(
                        unit = unit,
                        parametervalue = synt_params[param][1][parval]
                    )
                    session.add(parvalmap)

            def process_example(param_kw: str, ex_col: str, comment_col: str = ''):
                if param == param_kw and row[ex_col].strip() != '' and row[ex_col].strip() != 'NA':
                    ex = Example(
                        text = row[ex_col]
                    )
                    parvalmaps = [x for x in unit.parametervalue_mappings if x.parameter.keyword == param]
                    if len(parvalmaps) > 1:
                        print("WARNING: Example '%s' assigned to more than one value of parameter '%s' for unit '%s' at line %d" \
                            % (ex.text, param_kw, unit.linker, syntax.index(row)+2))
                    for parvalmap in parvalmaps:
                        parvalmap.examples.add(ex)
                    if comment_col != '' and row[comment_col].strip() != '':
                        parvalmap.comments.add(Comment(
                            text = row[comment_col]
                        ))
            process_example('parts.order', 'parts.order.example')
            process_example('linker_position', 'position.example')            
            process_example('clause.order', 'clause.order.example', 'clause order comments')
            process_example('indep.sentence', 'indep.sentence.example')

    # These parameters have to be done by hand because they are not regularly coded
    if row["inferential.example"] != '' and row["inferential.example"] != 'NA':
        parvalmap = UnitToParameterValue(
            unit = unit,
            parametervalue = inferential_param_yes
        )
        session.add(parvalmap)
        parvalmap.examples.add(Example(
            text = row['inferential.example']))
    else:
        parvalmap = UnitToParameterValue(
            unit = unit,
            parametervalue = inferential_param_no
        )
        session.add(parvalmap)

    if row["illoc example"] != '' and row["illoc example"] != 'NA':
        parvalmap = UnitToParameterValue(
            unit = unit,
            parametervalue = illoc_param_yes
        )
        session.add(parvalmap)
        parvalmap.examples.add(Example(
            text = row['illoc example']))
    else:
        parvalmap = UnitToParameterValue(
            unit = unit,
            parametervalue = illoc_param_no
        )
        session.add(parvalmap)

    if row["metatext example"] != '' and row["metatext example"] != 'NA':
        parvalmap = UnitToParameterValue(
            unit = unit,
            parametervalue = metatext_param_yes
        )
        session.add(parvalmap)
        parvalmap.examples.add(Example(
            text = row['metatext example']))       
    else:
        parvalmap = UnitToParameterValue(
            unit = unit,
            parametervalue = metatext_param_no
        )
        session.add(parvalmap)                 

    mainpart_text = row["mainpart"].split("; ")[0]
    if mainpart_text != '' and mainpart_text != 'NA':
        mainpart = Form(
            text = mainpart_text,
            formtype = type_mainpart,
            unit = unit
        )
        session.add(mainpart)
        unit.forms.add(mainpart)

    for param in synt_text_params.keys():
        if row[param] != '' and row[param] != 'NA':
            unit.textparametervalues.add(
                UnitToTextParameter(
                    parameter = synt_text_params[param],
                    value = row[param]
                )
            )
    
    if row["correl"].strip() != 'NA' and row["correl"].strip() != '':
        cortext = row["correl"]
        if cortext == 'дублирование':
            correl_form = unit.linker
        correl = Form(
                    text = cortext,
                    formtype = type_correl,
                    unit = unit,
                )
        
        correl.textparametervalues.add(FormToTextParameter(
            parameter = correl_text_params["correl.oblig"],
            value = row["correl.oblig"]
        ))

        # if "нет" in row["correl.oblig"]:
        #     corvalmap = FormToParameterValue(
        #         form = correl,
        #         parametervalue = correl_params["correl.oblig"][1]["нет"]
        #     )
        # else:
        #     corvalmap = FormToParameterValue(
        #         form = correl,
        #         parametervalue = correl_params["correl.oblig"][1]["да"]
        #     )

        if row["correl.oblig.example"].strip() != '':            
            correl.examples.add(
                Example(text = row["correl.oblig.example"])
            )

        if row["correl.position"].strip() != 'NA' and row["correl.position"].strip() != '':
            corvalmap = FormToParameterValue(
                form = correl,
                parametervalue = correl_params["correl.position"][1][row["correl.position"]]
            )
            if row["correl.position.example"].strip() != 'NA' and row["correl.position.example"].strip() != '':
                corvalmap.examples.add(
                    Example(text = row["correl.position.example"])
                )
            correl.parametervalue_mappings.add(corvalmap)

    for comm in row["comment"].split("; "):
        if comm != 'NA' and comm != '':
            unit.comments.add(Comment(
                text = comm
            ))

    session.add(unit)

# # Test Unit
# unit = Unit()
# session.add(unit)
# linker = Form(
#     text = "hello",
#     formtype = type_linker,
#     unit = unit
# )
# session.add(linker)

# correl = Form(
#     text = "world",
#     formtype = type_correl,
#     unit = unit
# )
# session.add(correl)

# ex = Example(
#     text = "FOOBAR"
# )

# valuemap = FormToParameterValue(
#     parametervalue = correl_params["correl.oblig"][1]["да"]
# )

# valuemap.examples.append(ex)

# correl.parametervalue_mappings.append(valuemap)

# unit.parametervalues.append(synt_params["parts.num"][1]["mono"])

# print(unit.parametervalue_mappings)

session.commit()

hyperlink_type = UnitLinkType(
    name='перекрёстная ссылка',
    keyword='hyperlink'
)
session.add(hyperlink_type)

for row in data:
    if row["Non-connector"] != "NA" and row["Non-connector"] != '' and row["Non-connector"] != 'объед':
        continue
    field = session.scalars(select(Semfield).where(Semfield.keyword == row['semfield1_ed'])).first()
    
    subfields = set()
    for sf in row["subfield1_ed"].split("; "):
        subfields.add(session.scalars(select(Subfield).where(Subfield.keyword == sf)).first())
    if field is None:
        print('WARNING: No such semantic field %s (unit %s)' % (row["semfield1_ed"], row["form"]))
        continue
    if row["edit form"] != '':
        search = row["edit form"]
    else: search = row["form"]
    units = session.scalars(select(Unit).where(
        Unit.linker == search)).all()
    if len(units) == 0:
        # print('WARNING: No unit %s in syntactic database!' % row["form"])
        # print('WARNING: No unit %s with semantic field %s!' % (row["form"], field.name))
        continue
    # unit_query = select(Unit).where(Unit.semfield == field)
    # field_units = session.scalars(unit_query).all()
    field_units = [u for u in units if u.semfield == field]

    if len(field_units) == 0:
        # print("WARNING: Unit %s is not found in syntactic database with semfield %s" % \
        #     (row["form"], row["semfield1_ed"]))
        continue

    if len(field_units) > 1 and len(subfields) > 0:
        for subfield in subfields:
            field_units = [u for u in field_units if subfield in u.subfields]
            if len(field_units) == 1: break

    if len(field_units) == 0:
        # print("WARNING: Unit %s is not found in syntactic database with semfield %s and subfields %s" % \
        #     (row["form"], row["semfield1_ed"], row["subfield1_ed"]))
        continue

    if len(field_units) > 1:
        # print("WARNING: Unit %s with semfield %s and subfields %s multiply defined in syntactic database." % \
        #     (row["form"], row["semfield1_ed"], row["subfield1_ed"]))
        continue

    unit = field_units[0]

    if row["edit form"] != '' and row["edit form"] not in [f.text for f in unit.forms if f.formtype.keyword == 'phonvar']:
        unit.forms.add(
            Form(
                text = row["form"],
                formtype = type_phonvar
            )
        )

    if row["hyperlink"] != '' and row["hyperlink"] != 'NA':
        refunits = session.scalars(select(Unit).where(Unit.linker == row['hyperlink'])).all()
        if len(refunits) == 0:
            print("WARNING! Referenced unit %s not found" % row["hyperlink"])
        else:
            if len(refunits) > 1:
                refunits = [u for u in refunits if u.semfield == field]
                if len(refunits) == 0:
                    print("WARNING! No referenced unit %s with semfield %s" % \
                        (row["hyperlink"], row["semfield1_ed"]))
                if len(refunits) > 1:
                    print("WARNING! More than one referenced unit %s with semfield %s" % \
                        (row["hyperlink"], row["semfield1_ed"]))
            # Check for duplicates, because sets are not reliable criteria for relationship identity
            if len(refunits) > 0 and refunits[0] not in {x.target for x in unit.links}:
                unit.links.add(UnitToUnit(
                    target = refunits[0],
                    unitlinktype = hyperlink_type
                ))
            
    # for param in sem_params.keys():
    #     if row[param] != '' and row[param] != 'NA':
    #         unit.parametervalues.add(sem_params[param][1][row[param]])

    # for textparam in sem_text_params.keys():
    #     if row[textparam] != '' and row[textparam] != 'NA':
    #         unit.textparametervalues.add(
    #             UnitToTextParameter(
    #                 parameter = sem_text_params[textparam],
    #                 value = row[textparam]
    #             )
    #         )

    # Stylistic constraints and semantic comments are supposed to be hardcoded
    if row["sem_comment"] != '' and row["sem_comment"] != 'NA': unit.sem_comment = row["sem_comment"]
    if row["Стилистич. ограничения"] != '' and row["Стилистич. ограничения"] != 'NA':unit.style = row["Стилистич. ограничения"]

    if row["Example"] != '' and row["Example"] != 'NA':
        ex = session.scalars(select(Example).where(Example.text == row["Example"])).first()
        if ex is None:
            ex = Example(
                text = row["Example"]
            )
            session.add(ex)
        unit.examples.add(ex)

    # semfield2_ed, subfield2_ed
    semfield_kw = row["semfield2_ed"]
    if semfield_kw != '' and semfield_kw != "NA":
        if semfield_kw in semfields_dict.keys():
            unit.extra_semfields.add(semfields_dict[semfield_kw])
        else:
            semfield = Semfield(
                keyword=semfield_kw,
                name=semfield_kw
            )
            session.add(semfield)
            unit.extra_semfields.add(semfield)
            semfields_dict[semfield_kw] = semfield

    subfield_kw = row["subfield2_ed"]
    if subfield_kw != '' and subfield_kw != "NA":
        for kw in subfield_kw.replace(",",";").split(";"):
            if kw in subfields_dict.keys():
                unit.subfields.add(subfields_dict[kw])
            else:
                subfield = Subfield(
                    keyword=kw,
                    name=kw,
                    semfield=semfields_dict[semfield_kw]
                )
                session.add(subfield)
                unit.subfields.add(subfield)
                subfields_dict[kw] = subfield

    # sem_comment
    sem_comment = row["sem_comment"]
    if sem_comment != '' and sem_comment != 'NA':
        comm = session.scalars(select(Comment).where(Comment.text == sem_comment)).first()
        if comm is None:
            comm = Comment(
                text = sem_comment,
                hidden = False
            )
            session.add(comm)
        unit.comments.add(comm)

    # inside_info -- hidden comment
    inside_info = row["inside_info"]
    if inside_info != '' and inside_info != 'NA':
        comm = session.scalars(select(Comment).where(Comment.text == inside_info)).first()
        if comm is None:
            comm = Comment(
                text = inside_info,
                hidden = True
            )
            session.add(comm)
        unit.comments.add(comm)

    # phonvar -- мне кажется, всё-таки не к словарной информации
    phonvar = row["phonvar"]
    if phonvar != '' and phonvar != 'NA' and phonvar not in [f.text for f in unit.forms if f.formtype.keyword == 'phonvar']:
        unit.forms.add(
            Form(
                text = phonvar,
                formtype = type_phonvar
            )
        )
    if row["dict"] == '': src = 'ИМК'
    else: src = row["dict"]
    meaning = Meaning(
        unit=unit,
        source=sources_dict[src],
        meaning = row["meaning"],
        pos = row["pos"],
        pos_type = row["type of pos"],
        other_senses = row["other_senses"],
        other_pos = row["other_pos"]
    )
    session.add(meaning)

    # к словарям:
    # pos, type of pos, meaning, other_senses, other_pos

session.commit()