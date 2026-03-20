from typing import Optional
import datetime
import decimal

from sqlalchemy import CHAR, DECIMAL, DateTime, Double, Float, ForeignKeyConstraint, Index, Integer, String, TIMESTAMP, text
from sqlalchemy.dialects.mysql import LONGBLOB, SMALLINT, TINYINT
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# sqlacodegen mysql+pymysql://root:root@localhost:3306/demo-db-345 > 345_model.py

class Base(DeclarativeBase):
    pass


class DuplicateBeakerReason(Base):
    __tablename__ = 'duplicate_beaker_reason'
    __table_args__ = {'comment': 'Table containing information of reason why PlateLotSensor of '
                'aborted beaker are reused. Rows are written to this table when '
                'the Proceed button is pressed on retest reason dialog.'}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    identity: Mapped[str] = mapped_column(String(100), nullable=False, comment='User name')
    reason: Mapped[str] = mapped_column(String(2000), nullable=False, comment='Reason for reruning beaker with already used lot parameters')
    duplicate_beakers: Mapped[str] = mapped_column(String(100), nullable=False, comment='Beakers used same lot parameters')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')

    beaker: Mapped[list['Beaker']] = relationship('Beaker', back_populates='dup_reason')


class EisFreqMap(Base):
    __tablename__ = 'eis_freq_map'
    __table_args__ = {'comment': 'Stores the mapping used between EIS index and frequency'}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    name: Mapped[str] = mapped_column(String(50), nullable=False, comment='Name of the EIS frequency map')
    frequencies: Mapped[str] = mapped_column(String(512), nullable=False, comment='Frequencies in a comma-separated list')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')

    measurement_limits: Mapped[list['MeasurementLimits']] = relationship('MeasurementLimits', back_populates='eis_freq_map')
    recipe: Mapped[list['Recipe']] = relationship('Recipe', back_populates='eis_freq_map')


class EisSpec(Base):
    __tablename__ = 'eis_spec'
    __table_args__ = {'comment': 'Stores EIS specs used in recipes'}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    name: Mapped[str] = mapped_column(String(50), nullable=False, comment='Name of this EIS spec')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')

    eis_limit: Mapped[list['EisLimit']] = relationship('EisLimit', back_populates='spec')
    measurement_limits: Mapped[list['MeasurementLimits']] = relationship('MeasurementLimits', back_populates='eis_spec')
    sequence: Mapped[list['Sequence']] = relationship('Sequence', back_populates='eisspec')


class Injector(Base):
    __tablename__ = 'injector'
    __table_args__ = (
        Index('first_subscription_time', 'first_subscription_time'),
        Index('serial', 'serial'),
        Index('session_start_time', 'session_start_time'),
        Index('test', 'test'),
        {'comment': 'Injector data'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    test: Mapped[str] = mapped_column(String(200), nullable=False, comment='Name of the test')
    serial: Mapped[str] = mapped_column(String(40), nullable=False, comment='Serial of the GST')
    first_subscription_time: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, comment='Time of first subscription')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    session_start_time: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, comment='Session start time')


class SchemaVersion(Base):
    __tablename__ = 'schema_version'
    __table_args__ = {'comment': 'Information on database schema versions'}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    version: Mapped[int] = mapped_column(Integer, nullable=False, comment='Number of this version')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    description: Mapped[Optional[str]] = mapped_column(String(500), comment='Description of this version')


class Station(Base):
    __tablename__ = 'station'
    __table_args__ = (
        Index('unique_index', 'name', 'address', 'dongle', 'injected', unique=True),
        {'comment': 'Information on stations'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    name: Mapped[str] = mapped_column(String(50), nullable=False, comment='Name of the station')
    address: Mapped[str] = mapped_column(String(40), nullable=False, comment='Address for this station')
    injected: Mapped[int] = mapped_column(TINYINT, nullable=False)
    dongle: Mapped[str] = mapped_column(String(20), nullable=False, comment='Dongle at this station')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')

    gst_group: Mapped[list['GstGroup']] = relationship('GstGroup', back_populates='station')
    beaker: Mapped[list['Beaker']] = relationship('Beaker', back_populates='station')


class Symbols(Base):
    __tablename__ = 'symbols'
    __table_args__ = {'comment': 'Small configurable image, used in execution recipes. For better '
                'report recognition.'}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    check_sum: Mapped[str] = mapped_column(String(100), nullable=False, comment='Checksum of image')
    data: Mapped[bytes] = mapped_column(LONGBLOB, nullable=False, comment='Serialized image')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')

    exec_recipe: Mapped[list['ExecRecipe']] = relationship('ExecRecipe', back_populates='symbol')


class EisLimit(Base):
    __tablename__ = 'eis_limit'
    __table_args__ = (
        ForeignKeyConstraint(['spec_id'], ['eis_spec.id'], name='eis_limit_ibfk_1'),
        Index('spec_id', 'spec_id'),
        {'comment': 'Stores EIS limits used in recipes'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    frequency: Mapped[int] = mapped_column(Integer, nullable=False, comment='EIS frequency index')
    spec_id: Mapped[int] = mapped_column(Integer, nullable=False, comment='ID of the EIS spec this limit is part of')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    min_real: Mapped[Optional[int]] = mapped_column(Integer, comment='Min allowed value for real component of frequency (null if no limit)')
    max_real: Mapped[Optional[int]] = mapped_column(Integer, comment='Max allowed value for real component of frequency (null if no limit)')
    min_imaginary: Mapped[Optional[int]] = mapped_column(Integer, comment='Min allowed value for imaginary component of frequency (null if no limit)')
    max_imaginary: Mapped[Optional[int]] = mapped_column(Integer, comment='Max allowed value for imaginary component of frequency (null if no limit)')

    spec: Mapped['EisSpec'] = relationship('EisSpec', back_populates='eis_limit')


class GstGroup(Base):
    __tablename__ = 'gst_group'
    __table_args__ = (
        ForeignKeyConstraint(['station_id'], ['station.id'], name='gst_group_ibfk_1'),
        Index('active', 'active'),
        Index('beaker_index', 'beaker_index'),
        Index('injected', 'injected'),
        Index('machine', 'machine'),
        Index('station_id', 'station_id'),
        Index('status', 'status'),
        Index('time_finished', 'time_finished'),
        Index('unique_name', 'name', 'active', unique=True),
        {'comment': 'Group of GSTs during Plug Test'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    name: Mapped[str] = mapped_column(String(200), nullable=False, comment='Name of the GST group.  Will not be empty')
    status: Mapped[str] = mapped_column(CHAR(1), nullable=False, comment='Status of this group.  P = in Plug test; T = plug Tested; B = in Beaker test; C = Complete; E = Emptied')
    time_finished: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, comment='Time when the plug test was completed')
    injected: Mapped[int] = mapped_column(TINYINT, nullable=False, comment='Was this GST group tested using Injected (fake) data')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    beaker_index: Mapped[Optional[int]] = mapped_column(Integer, comment='Index of this group in its beaker (null if not in a beaker yet)')
    station_id: Mapped[Optional[int]] = mapped_column(Integer, comment='ID of the station on which the plug test was performed (null for Synergy or SSDU)')
    active: Mapped[Optional[int]] = mapped_column(TINYINT, comment='1 when group is active (InPlugTest, PlugTested, or InBeakerTest); null otherwise')
    machine: Mapped[Optional[str]] = mapped_column(String(100), comment='Name of machine on which this group was plug tested')
    transmitter: Mapped[Optional[str]] = mapped_column(String(20), comment='Type of transmitter in this group (GST1, GST5C, Synergy, GST5G)')

    station: Mapped[Optional['Station']] = relationship('Station', back_populates='gst_group')
    tested_gst: Mapped[list['TestedGst']] = relationship('TestedGst', back_populates='gst_group')


class MeasurementLimits(Base):
    __tablename__ = 'measurement_limits'
    __table_args__ = (
        ForeignKeyConstraint(['eis_freq_map_id'], ['eis_freq_map.id'], name='measurement_limits_ibfk_2'),
        ForeignKeyConstraint(['eis_spec_id'], ['eis_spec.id'], name='measurement_limits_ibfk_1'),
        Index('eis_freq_map_id', 'eis_freq_map_id'),
        Index('eis_spec_id', 'eis_spec_id'),
        {'comment': 'Stores measurement limits for plug testing'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    vbatt_low: Mapped[decimal.Decimal] = mapped_column(Double(asdecimal=True), nullable=False)
    isig_low: Mapped[decimal.Decimal] = mapped_column(Double(asdecimal=True), nullable=False)
    isig_high: Mapped[decimal.Decimal] = mapped_column(Double(asdecimal=True), nullable=False)
    vcntr_low: Mapped[decimal.Decimal] = mapped_column(Double(asdecimal=True), nullable=False)
    vcntr_high: Mapped[decimal.Decimal] = mapped_column(Double(asdecimal=True), nullable=False)
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    name: Mapped[Optional[str]] = mapped_column(String(50))
    eis_spec_id: Mapped[Optional[int]] = mapped_column(Integer)
    eis_freq_map_id: Mapped[Optional[int]] = mapped_column(Integer)

    eis_freq_map: Mapped[Optional['EisFreqMap']] = relationship('EisFreqMap', back_populates='measurement_limits')
    eis_spec: Mapped[Optional['EisSpec']] = relationship('EisSpec', back_populates='measurement_limits')
    tested_gst: Mapped[list['TestedGst']] = relationship('TestedGst', back_populates='measurement_limits')


class Recipe(Base):
    __tablename__ = 'recipe'
    __table_args__ = (
        ForeignKeyConstraint(['eis_freq_map_id'], ['eis_freq_map.id'], name='recipe_ibfk_1'),
        Index('eis_freq_map_id', 'eis_freq_map_id')
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment='Name of this part recipe')
    description: Mapped[str] = mapped_column(String(200), nullable=False, comment='Description of the part recipe')
    prod_recipe: Mapped[int] = mapped_column(TINYINT, nullable=False, comment='Defines if recipe is for production usage')
    traveler_lot_format: Mapped[str] = mapped_column(String(50), nullable=False, comment='Traveler lot format (human readable)')
    traveler_lot_format_regex: Mapped[str] = mapped_column(String(50), nullable=False, comment='Valid traveler lot format (regular expression)')
    sterilization_lot_format: Mapped[str] = mapped_column(String(50), nullable=False, comment='Valid sterilization lot format (human readable)')
    sterilization_lot_format_regex: Mapped[str] = mapped_column(String(50), nullable=False, comment='Valid sterilization lot format (regular expression)')
    min_temperature: Mapped[decimal.Decimal] = mapped_column(Double(asdecimal=True), nullable=False, comment='Minimum acceptable temperature for this recipe')
    max_temperature: Mapped[decimal.Decimal] = mapped_column(Double(asdecimal=True), nullable=False, comment='Maximum acceptable temperature for this recipe')
    station_type: Mapped[str] = mapped_column(String(10), nullable=False, comment='Transmitter type')
    test_type: Mapped[str] = mapped_column(CHAR(1), nullable=False, comment='Type of test (M = post-membrane, S = post-sterilization)')
    report_type: Mapped[int] = mapped_column(Integer, nullable=False, comment='Type of generated report')
    sequence_recipe_name: Mapped[str] = mapped_column(String(100), nullable=False, comment='Name of the sequence recipe')
    active: Mapped[int] = mapped_column(TINYINT, nullable=False, comment='Active part recipe; if set to zero, this part recipe cannot be used for new beakers')
    ph_analyzers: Mapped[str] = mapped_column(String(500), nullable=False, comment='List of pH analyzers')
    dextrose_analyzers: Mapped[str] = mapped_column(String(500), nullable=False, comment='List of dextrose analyzers')
    hot_plates: Mapped[str] = mapped_column(String(500), nullable=False, comment='List of hot plates')
    dextrose_materials: Mapped[str] = mapped_column(String(500), nullable=False, comment='List of allowable dextrose materials')
    buffer_materials: Mapped[str] = mapped_column(String(500), nullable=False, comment='List of allowable buffer materials')
    linearity: Mapped[int] = mapped_column(TINYINT, nullable=False, comment='Use this recipe for Linearity?')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    eis_freq_map_id: Mapped[Optional[int]] = mapped_column(Integer, comment='ID of the EIS frequency map used in this recipe')
    market_category_lockout: Mapped[Optional[int]] = mapped_column(Integer, comment='Market Category Lockout (Synergy only)')

    eis_freq_map: Mapped[Optional['EisFreqMap']] = relationship('EisFreqMap', back_populates='recipe')
    beaker: Mapped[list['Beaker']] = relationship('Beaker', back_populates='recipe')
    exec_recipe: Mapped[list['ExecRecipe']] = relationship('ExecRecipe', back_populates='recipe')
    sequence: Mapped[list['Sequence']] = relationship('Sequence', back_populates='recipe')


class Beaker(Base):
    __tablename__ = 'beaker'
    __table_args__ = (
        ForeignKeyConstraint(['dup_reason_id'], ['duplicate_beaker_reason.id'], name='beaker_ibfk_3'),
        ForeignKeyConstraint(['recipe_id'], ['recipe.id'], name='beaker_ibfk_2'),
        ForeignKeyConstraint(['station_id'], ['station.id'], name='beaker_ibfk_1'),
        Index('dup_reason_id', 'dup_reason_id'),
        Index('machine', 'machine'),
        Index('recipe_id', 'recipe_id'),
        Index('start_time', 'start_time'),
        Index('station_id', 'station_id'),
        Index('sterilization_lot', 'sterilization_lot'),
        Index('stop_time', 'stop_time'),
        Index('test_type', 'test_type'),
        {'comment': 'Table containing information on beakers.  Rows are written to '
                'this table when the Finalize button is pressed.'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    tag: Mapped[str] = mapped_column(String(200), nullable=False, comment='Beaker tag')
    test_type: Mapped[str] = mapped_column(CHAR(1), nullable=False, comment='Type of test (M = post-membrane, S = post-sterilization')
    machine: Mapped[str] = mapped_column(String(80), nullable=False, comment='Name of the machine on which the Collator GUI was running for this beaker test')
    user: Mapped[str] = mapped_column(String(80), nullable=False, comment='Name of the user operating the Collator GUI for this beaker test')
    engineering_mode: Mapped[int] = mapped_column(TINYINT, nullable=False, comment='0 = live, 1 = engineering mode, 2 = injected data, 3 = injected in engineering mode')
    mes: Mapped[int] = mapped_column(TINYINT, nullable=False, comment='Was this beaker run in MES Mode')
    sterilization_lot: Mapped[str] = mapped_column(String(50), nullable=False, comment='Sterilization lot (empty if not a post-sterilization beaker test)')
    start_time: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, comment='Time when the beaker test was started')
    completed: Mapped[str] = mapped_column(CHAR(1), nullable=False, comment='Completion of the beaker.  N = not completed, Y = completed, A = aborted')
    bts_system: Mapped[str] = mapped_column(String(80), nullable=False, comment='BTS System Name on which the Collator GUI was running for this beaker test')
    station_id: Mapped[int] = mapped_column(Integer, nullable=False)
    recipe_id: Mapped[int] = mapped_column(Integer, nullable=False, comment='ID of the recipe used for this beaker test')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    comment: Mapped[Optional[str]] = mapped_column(String(200), comment='Beaker comment')
    stop_time: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, comment='Time when the beaker test was completed (null if test has not yet been completed)')
    mes_certified_user: Mapped[Optional[str]] = mapped_column(String(50), comment='If MES, name of certified user.  Null if not MES')
    mes_pod_name: Mapped[Optional[str]] = mapped_column(String(50), comment='POD name. Null if not MES.')
    mes_track_in: Mapped[Optional[str]] = mapped_column(String(1024), comment='MES Track In')
    mes_track_out: Mapped[Optional[str]] = mapped_column(String(1024), comment='MES Track Out')
    median_session_start_time: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    submersion_confirmed_time: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    dup_reason_id: Mapped[Optional[int]] = mapped_column(Integer, comment='ID of the duplication reason provided for beaker test')

    dup_reason: Mapped[Optional['DuplicateBeakerReason']] = relationship('DuplicateBeakerReason', back_populates='beaker')
    recipe: Mapped['Recipe'] = relationship('Recipe', back_populates='beaker')
    station: Mapped['Station'] = relationship('Station', back_populates='beaker')
    cgm_raw: Mapped[list['CgmRaw']] = relationship('CgmRaw', back_populates='beaker')
    diagnostic_event: Mapped[list['DiagnosticEvent']] = relationship('DiagnosticEvent', back_populates='beaker')
    gst1_raw: Mapped[list['Gst1Raw']] = relationship('Gst1Raw', back_populates='beaker')
    sequence_data: Mapped[list['SequenceData']] = relationship('SequenceData', back_populates='beaker')
    vbatt: Mapped[list['Vbatt']] = relationship('Vbatt', back_populates='beaker')
    eis_record: Mapped[list['EisRecord']] = relationship('EisRecord', back_populates='beaker')
    sensor: Mapped[list['Sensor']] = relationship('Sensor', back_populates='beaker')


class ExecRecipe(Base):
    __tablename__ = 'exec_recipe'
    __table_args__ = (
        ForeignKeyConstraint(['recipe_id'], ['recipe.id'], name='exec_recipe_ibfk_1'),
        ForeignKeyConstraint(['symbol_id'], ['symbols.id'], name='exec_recipe_ibfk_2'),
        Index('recipe_id', 'recipe_id'),
        Index('symbol_id', 'symbol_id'),
        {'comment': 'Stores execution recipes, used in recipes with report types 1 and '
                '2.'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment='Name of this execution recipe')
    report_type: Mapped[int] = mapped_column(Integer, nullable=False, comment='Report type')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    accept_number: Mapped[Optional[int]] = mapped_column(SMALLINT(unsigned=True), comment='Allowed number of rejected samples')
    size: Mapped[Optional[int]] = mapped_column(SMALLINT(unsigned=True), comment='Required number of samples')
    recipe_id: Mapped[Optional[int]] = mapped_column(Integer)
    symbol_id: Mapped[Optional[int]] = mapped_column(Integer, comment='Image db id')

    recipe: Mapped[Optional['Recipe']] = relationship('Recipe', back_populates='exec_recipe')
    symbol: Mapped[Optional['Symbols']] = relationship('Symbols', back_populates='exec_recipe')
    next_level_part: Mapped[list['NextLevelPart']] = relationship('NextLevelPart', back_populates='exec_recipe')
    sequence: Mapped[list['Sequence']] = relationship('Sequence', back_populates='exec_recipe')


class CgmRaw(Base):
    __tablename__ = 'cgm_raw'
    __table_args__ = (
        ForeignKeyConstraint(['beaker_id'], ['beaker.id'], name='cgm_raw_ibfk_1'),
        Index('beaker_id', 'beaker_id'),
        Index('measurement_time', 'measurement_time'),
        Index('ser_num', 'ser_num'),
        {'comment': 'Table containing raw CGM format data.  Stored at the end of the '
                'plug test, and for every CGM packet received during beaker text.'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    ser_num: Mapped[str] = mapped_column(String(64), nullable=False, comment='GST serial number.')
    time_received: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, comment='Time this data was received by the collator or SSDU')
    injected: Mapped[int] = mapped_column(TINYINT, nullable=False, comment='Was this data injected instead of live')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    beaker_id: Mapped[Optional[int]] = mapped_column(Integer, comment='Which beaker was this data acquired for (null if acquired for plug test)')
    isig: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 2), comment='Raw ISig value')
    vcntr: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 3), comment='Raw VCntr value')
    general_sensor_device_fault: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Columns between general_sensor_device_fault and above_valid_range are stored directly from the CGM message')
    calibration_recommended: Mapped[Optional[int]] = mapped_column(TINYINT)
    calibration_required: Mapped[Optional[int]] = mapped_column(TINYINT)
    type_incorrect_for_device: Mapped[Optional[int]] = mapped_column(TINYINT)
    sensor_temperature_too_high: Mapped[Optional[int]] = mapped_column(TINYINT)
    sensor_in_initialization: Mapped[Optional[int]] = mapped_column(TINYINT)
    device_battery_dead: Mapped[Optional[int]] = mapped_column(TINYINT)
    session_stopped: Mapped[Optional[int]] = mapped_column(TINYINT)
    sensor_temperature_too_low: Mapped[Optional[int]] = mapped_column(TINYINT)
    sensor_error_alert: Mapped[Optional[int]] = mapped_column(TINYINT)
    device_battery_low: Mapped[Optional[int]] = mapped_column(TINYINT)
    closed_loop: Mapped[Optional[int]] = mapped_column(TINYINT)
    battery_low: Mapped[Optional[int]] = mapped_column(TINYINT)
    change_sensor: Mapped[Optional[int]] = mapped_column(TINYINT)
    sensor_error: Mapped[Optional[int]] = mapped_column(TINYINT)
    device_specific_alert: Mapped[Optional[int]] = mapped_column(TINYINT)
    isig_discarded: Mapped[Optional[int]] = mapped_column(TINYINT)
    change_sensor_due_to_senor_error: Mapped[Optional[int]] = mapped_column(TINYINT)
    time_sync_required: Mapped[Optional[int]] = mapped_column(TINYINT)
    below_valid_range: Mapped[Optional[int]] = mapped_column(TINYINT)
    sensor_configuration_needed: Mapped[Optional[int]] = mapped_column(TINYINT)
    isig_noisy: Mapped[Optional[int]] = mapped_column(TINYINT)
    calibration_not_allowed: Mapped[Optional[int]] = mapped_column(TINYINT)
    sensor_malfunction: Mapped[Optional[int]] = mapped_column(TINYINT)
    above_valid_range: Mapped[Optional[int]] = mapped_column(TINYINT)
    measurement_time: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, comment='Time this data value was acquired by the transmitter')
    batt_volts: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(6, 3), comment='Most recent battery voltage (plug test only)')
    receive_state: Mapped[Optional[str]] = mapped_column(CHAR(1), comment='At what phase of the sequence was this data received: B=Before start, Z=Before stabilized, R=While restarting, S=Active sequence, D=Paused, E=After end, P=Plug test, U=SSDU, X=Unit test')
    collector_time: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, comment='Time this data value was acquired by the collector or SSDU')

    beaker: Mapped[Optional['Beaker']] = relationship('Beaker', back_populates='cgm_raw')
    tested_gst: Mapped[list['TestedGst']] = relationship('TestedGst', back_populates='cgm_raw')
    sequence_sensor_result: Mapped[list['SequenceSensorResult']] = relationship('SequenceSensorResult', back_populates='cgm_raw')


class DiagnosticEvent(Base):
    __tablename__ = 'diagnostic_event'
    __table_args__ = (
        ForeignKeyConstraint(['beaker_id'], ['beaker.id'], name='diagnostic_event_ibfk_1'),
        Index('beaker_id', 'beaker_id'),
        {'comment': 'Diagnostic events'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    time_reported: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, comment='Time the event was reported')
    event_type: Mapped[str] = mapped_column(String(50), nullable=False, comment='Type of the diagnostic event')
    details: Mapped[str] = mapped_column(String(512), nullable=False, comment='Details of the diagnostic event')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    beaker_id: Mapped[Optional[int]] = mapped_column(Integer, comment='ID of the beaker (null if none)')

    beaker: Mapped[Optional['Beaker']] = relationship('Beaker', back_populates='diagnostic_event')


class Gst1Raw(Base):
    __tablename__ = 'gst1_raw'
    __table_args__ = (
        ForeignKeyConstraint(['beaker_id'], ['beaker.id'], name='gst1_raw_ibfk_1'),
        Index('beaker_id', 'beaker_id'),
        Index('ser_num', 'ser_num'),
        Index('time_acquired', 'time_acquired'),
        {'comment': 'Table containing raw GST1 format data.  Stored at the end of the '
                'plug test, and for every GST1 packet received during beaker text.'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    time_acquired: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, comment='Time the GST reported the data had been acquired')
    time_received: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, comment='Time the data was received by the collator')
    injected: Mapped[int] = mapped_column(TINYINT, nullable=False, comment='Was this data injected instead of live')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    ser_num: Mapped[Optional[str]] = mapped_column(String(64), comment='GST serial')
    beaker_id: Mapped[Optional[int]] = mapped_column(Integer, comment='Which beaker was this data acquired for (null if acquired for plug test)')
    isig_now: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 2), comment='Value for ISigNow reported in the GST1 message')
    isig_prev: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 2), comment='Value for ISigPrev reported in the GST1 message')
    vctr_now: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 3), comment='Value for VCTR_Now reported in the GST1 message')
    vctr_prev: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 3), comment='Value for VCTR_Prev reported in the GST1 message')
    batt_volts: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 3), comment='Value for BattVolts reported in the GST1 message')
    counts_now: Mapped[Optional[int]] = mapped_column(Integer, comment='Value for CountsNow reported in the GST1 message')
    counts_prev: Mapped[Optional[int]] = mapped_column(Integer, comment='Value for CountsPrev reported in the GST1 message')
    c1: Mapped[Optional[int]] = mapped_column(Integer, comment='Value for C1 reported in the GST1 message')
    c2: Mapped[Optional[int]] = mapped_column(Integer, comment='Value for C2 reported in the GST1 message')
    software_version: Mapped[Optional[str]] = mapped_column(String(30), comment='Value for SoftwareVersion reported in the GST1 message')
    low_batt: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Value for LowBatt reported in the GST1 message')
    dead_batt: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Value for DeadBatt reported in the GST1 message')
    dry_sensor: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Value for DrySensor reported in the GST1 message')
    discard_now: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Value for DiscardNow reported in the GST1 message')
    noise_now: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Value for NoiseNow reported in the GST1 message')
    overflow_now: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Value for OverflowNow reported in the GST1 message')
    discard_prev: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Value for DiscardPrev reported in the GST1 message')
    noise_prev: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Value for NoisePrev reported in the GST1 message')
    overflow_prev: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Value for OverflowPrev reported in the GST1 message')
    repeat_msg: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Value for RepeatMsg reported in the GST1 message')
    flags_now: Mapped[Optional[str]] = mapped_column(String(80), comment='Value for FlagsNow reported in the GST1 message')
    flags_prev: Mapped[Optional[str]] = mapped_column(String(80), comment='Value for FlagsPrev reported in the GST1 message')
    status_flags: Mapped[Optional[str]] = mapped_column(String(80), comment='Value for StatusFlags reported in the GST1 message')
    corrupt_crc: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Value for CorruptCRC reported in the GST1 message')
    humidity_counts: Mapped[Optional[int]] = mapped_column(Integer, comment='Value for HumidityCount reported in the GST1 message')
    humidity_value: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 2), comment='Value for HumidityValue reported in the GST1 message')
    temp_counts: Mapped[Optional[int]] = mapped_column(Integer, comment='Value for TempCounts reported in the GST1 message')
    temp_value: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 2), comment='Value for TempValue reported in the GST1 message')

    beaker: Mapped[Optional['Beaker']] = relationship('Beaker', back_populates='gst1_raw')
    tested_gst: Mapped[list['TestedGst']] = relationship('TestedGst', back_populates='gst1_raw')
    sequence_sensor_result: Mapped[list['SequenceSensorResult']] = relationship('SequenceSensorResult', back_populates='gst1_raw')


class NextLevelPart(Base):
    __tablename__ = 'next_level_part'
    __table_args__ = (
        ForeignKeyConstraint(['exec_recipe_id'], ['exec_recipe.id'], name='next_level_part_ibfk_1'),
        Index('exec_recipe_id', 'exec_recipe_id'),
        {'comment': 'Stores execution next partition numbers, used in execution '
                'recipes. Used only for diplaying in reports with report types 1 '
                'and 2.'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    part_number: Mapped[str] = mapped_column(String(100), nullable=False, comment='Name of the next part recipe')
    part_description: Mapped[str] = mapped_column(String(200), nullable=False, comment='Description of the next part recipe')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    exec_recipe_id: Mapped[Optional[int]] = mapped_column(Integer)

    exec_recipe: Mapped[Optional['ExecRecipe']] = relationship('ExecRecipe', back_populates='next_level_part')


class Sequence(Base):
    __tablename__ = 'sequence'
    __table_args__ = (
        ForeignKeyConstraint(['eisspec_id'], ['eis_spec.id'], name='sequence_ibfk_3'),
        ForeignKeyConstraint(['exec_recipe_id'], ['exec_recipe.id'], name='sequence_ibfk_2'),
        ForeignKeyConstraint(['recipe_id'], ['recipe.id'], name='sequence_ibfk_1'),
        Index('eisspec_id', 'eisspec_id'),
        Index('exec_recipe_id', 'exec_recipe_id'),
        Index('recipe_id', 'recipe_id')
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    recipe_id: Mapped[int] = mapped_column(Integer, nullable=False)
    order_in_recipe: Mapped[int] = mapped_column(Integer, nullable=False, comment='Order of this sequence in its containing recipe (first sequence = 0)')
    min_isig: Mapped[decimal.Decimal] = mapped_column(Double(asdecimal=True), nullable=False, comment='Minimum acceptable value for ISig')
    max_isig: Mapped[decimal.Decimal] = mapped_column(Double(asdecimal=True), nullable=False, comment='Maximum acceptable value for ISig')
    is_linearity: Mapped[int] = mapped_column(TINYINT, nullable=False, comment='Use this sequence for linearity calculation? Only checked when generating linearity report')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    exec_recipe_id: Mapped[Optional[int]] = mapped_column(Integer)
    name: Mapped[Optional[str]] = mapped_column(String(50), comment='Name of this sequence')
    target_dextrose: Mapped[Optional[decimal.Decimal]] = mapped_column(Double(asdecimal=True), comment='Target dextrose')
    target_o2: Mapped[Optional[decimal.Decimal]] = mapped_column(Double(asdecimal=True), comment='Target O2')
    min_dextrose: Mapped[Optional[decimal.Decimal]] = mapped_column(Double(asdecimal=True), comment='Minimum acceptable dextrose level')
    max_dextrose: Mapped[Optional[decimal.Decimal]] = mapped_column(Double(asdecimal=True), comment='Maximum acceptable dextrose level')
    min_ph: Mapped[Optional[decimal.Decimal]] = mapped_column(Double(asdecimal=True), comment='Minimum acceptable pH level')
    max_ph: Mapped[Optional[decimal.Decimal]] = mapped_column(Double(asdecimal=True), comment='Maximum acceptable pH level')
    min_isig_roc: Mapped[Optional[decimal.Decimal]] = mapped_column(Double(asdecimal=True), comment='Minimum acceptable value for ISig RoC (null if no limit)')
    max_isig_roc: Mapped[Optional[decimal.Decimal]] = mapped_column(Double(asdecimal=True), comment='Maximum acceptable value for ISig RoC (null if no limit)')
    min_wait_minutes: Mapped[Optional[decimal.Decimal]] = mapped_column(Double(asdecimal=True), comment='Minimum wait (stabilization) time, in minutes')
    max_duration_minutes: Mapped[Optional[decimal.Decimal]] = mapped_column(Double(asdecimal=True), comment='Maximum sequence duration time, in minutes')
    eisspec_name: Mapped[Optional[str]] = mapped_column(String(50), comment='Name of the EIS spec used for this recipe')
    min_vcntr: Mapped[Optional[decimal.Decimal]] = mapped_column(Double(asdecimal=True), comment='Minimum acceptable value for VCntr (null if no limit)')
    max_vcntr: Mapped[Optional[decimal.Decimal]] = mapped_column(Double(asdecimal=True), comment='Maximum acceptable value for VCntr (null if no limit)')
    min_vcntr_roc: Mapped[Optional[decimal.Decimal]] = mapped_column(Double(asdecimal=True), comment='Minimum acceptable value for VCntr RoC (in decimal so 0.5 = 50%) (null if no limit)')
    max_vcntr_roc: Mapped[Optional[decimal.Decimal]] = mapped_column(Double(asdecimal=True), comment='Maximum acceptable value for VCntr RoC (in decimal so 0.5 = 50%) (null if no limit)')
    eisspec_id: Mapped[Optional[int]] = mapped_column(Integer, comment='ID of the EIS spec used in this recipe')
    mes_sequence_name: Mapped[Optional[str]] = mapped_column(String(50), comment='Name of the sequence in MES (MPROC), null if not MPROC')

    eisspec: Mapped[Optional['EisSpec']] = relationship('EisSpec', back_populates='sequence')
    exec_recipe: Mapped[Optional['ExecRecipe']] = relationship('ExecRecipe', back_populates='sequence')
    recipe: Mapped['Recipe'] = relationship('Recipe', back_populates='sequence')


class SequenceData(Base):
    __tablename__ = 'sequence_data'
    __table_args__ = (
        ForeignKeyConstraint(['beaker_id'], ['beaker.id'], name='sequence_data_ibfk_1'),
        Index('beaker_id', 'beaker_id'),
        Index('sequence_stop_mark', 'sequence_stop_mark'),
        Index('step', 'step'),
        {'comment': 'Information on a single sequence in a beaker test'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    beaker_id: Mapped[int] = mapped_column(Integer, nullable=False, comment='ID of the row in the beaker table describing the beaker test this sequence is a part of')
    step: Mapped[int] = mapped_column(Integer, nullable=False, comment='Order of this sequence in the beaker (zero-based)')
    comment: Mapped[str] = mapped_column(String(300), nullable=False, comment='Sequence comment')
    time_started: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, comment='Time the sequence was started')
    sequence_stop_mark: Mapped[str] = mapped_column(CHAR(1), nullable=False, comment='Why this sequence stopped.  N = Not yet complete, G = All GSTs good, T = maximum time reached, A = Aborted, C = Complete by operator, P = Max time while paused')
    ph_analyzer: Mapped[str] = mapped_column(String(80), nullable=False, comment='Model number of the analyzer used to measure the pH')
    dextrose_analyzer: Mapped[str] = mapped_column(String(80), nullable=False, comment='Model number of the analyzer used to measure the dextrose')
    hot_plate: Mapped[str] = mapped_column(String(80), nullable=False, comment='Model number of the hot plate used')
    dextrose_material: Mapped[str] = mapped_column(String(200), nullable=False, comment='Dextrose material used')
    dextrose_batch: Mapped[str] = mapped_column(String(200), nullable=False, comment='Dextrose batch used')
    buffer_material: Mapped[str] = mapped_column(String(200), nullable=False, comment='Buffer material used')
    buffer_batch: Mapped[str] = mapped_column(String(200), nullable=False, comment='Buffer batch used')
    time_stabilized: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, comment='Time the sequence reached min time')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    time_stopped: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, comment='Time the sequence was stopped (null if started but not yet stopped)')
    dextrose: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 2), comment='Measured dextrose value (entered by operator)')
    ph: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 2), comment='Measured pH value (entered by operator)')
    temperature: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 2), comment='Measured temperature (entered by operator)')
    ph_monitor_lot: Mapped[Optional[str]] = mapped_column(String(100), comment='pH Monitor lot (MES only)')
    dextrose_monitor_lot: Mapped[Optional[str]] = mapped_column(String(100), comment='Dextrose Monitor lot (MES only)')
    ph_expiration: Mapped[Optional[str]] = mapped_column(String(100), comment='Ph Monitor lot expiration time (MES CLS only)')
    dextrose_expiration: Mapped[Optional[str]] = mapped_column(String(100), comment='Dextrose Monitor lot expiration time (MES CLS only)')

    beaker: Mapped['Beaker'] = relationship('Beaker', back_populates='sequence_data')
    eis_record: Mapped[list['EisRecord']] = relationship('EisRecord', back_populates='sequence_data')
    sequence_sensor_result: Mapped[list['SequenceSensorResult']] = relationship('SequenceSensorResult', back_populates='sequence_data')


class Vbatt(Base):
    __tablename__ = 'vbatt'
    __table_args__ = (
        ForeignKeyConstraint(['beaker_id'], ['beaker.id'], name='vbatt_ibfk_1'),
        Index('beaker_id', 'beaker_id'),
        {'comment': 'Information on battery voltages'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    serial: Mapped[str] = mapped_column(String(64), nullable=False, comment='Transmitter serial number')
    vbatt: Mapped[float] = mapped_column(Float, nullable=False, comment='Battery voltage')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    time_acquired: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, comment='Time when the battery voltage was acquired by the transmitter')
    beaker_id: Mapped[Optional[int]] = mapped_column(Integer, comment='Beaker test')

    beaker: Mapped[Optional['Beaker']] = relationship('Beaker', back_populates='vbatt')


class EisRecord(Base):
    __tablename__ = 'eis_record'
    __table_args__ = (
        ForeignKeyConstraint(['beaker_id'], ['beaker.id'], name='eis_record_ibfk_1'),
        ForeignKeyConstraint(['sequence_data_id'], ['sequence_data.id'], name='eis_record_ibfk_2'),
        Index('beaker_id', 'beaker_id'),
        Index('sequence_data_id', 'sequence_data_id'),
        Index('ser_num', 'ser_num'),
        Index('time_acquired', 'time_acquired'),
        {'comment': 'Record grouping all EIS data for a single measurement.  The raw '
                'data for each frequency is stored in individual rows in the '
                'eis_raw table.'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    ser_num: Mapped[str] = mapped_column(String(64), nullable=False, comment='GST serial number')
    time_acquired: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, comment='Time this data was acquired (according to the GST)')
    when_received: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, comment='Time this data was received by the collator or SSDU')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    beaker_id: Mapped[Optional[int]] = mapped_column(Integer, comment='Which beaker was this data acquired for (null if acquired for plug test)')
    sequence_data_id: Mapped[Optional[int]] = mapped_column(Integer, comment='Which sequence was this data acquired for (null if acquired for plug test)')
    receive_state: Mapped[Optional[str]] = mapped_column(CHAR(1), comment='At what phase of the sequence was this data received: B=Before start, Z=Before stabilized, R=While restarting, S=Active sequence, D=Paused, E=After end, P=Plug test, U=SSDU, X=Unit test')
    injected: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Was this data injected instead of live')

    beaker: Mapped[Optional['Beaker']] = relationship('Beaker', back_populates='eis_record')
    sequence_data: Mapped[Optional['SequenceData']] = relationship('SequenceData', back_populates='eis_record')
    eis_raw: Mapped[list['EisRaw']] = relationship('EisRaw', back_populates='eis_record')
    tested_gst: Mapped[list['TestedGst']] = relationship('TestedGst', back_populates='eis_record')
    sequence_sensor_result: Mapped[list['SequenceSensorResult']] = relationship('SequenceSensorResult', back_populates='eis_record')


class EisRaw(Base):
    __tablename__ = 'eis_raw'
    __table_args__ = (
        ForeignKeyConstraint(['eis_record_id'], ['eis_record.id'], name='eis_raw_ibfk_1'),
        Index('eis_record_id', 'eis_record_id'),
        {'comment': 'Table containing raw EIS format data for a single frequency.  '
                'Stored at the end of the plug test, and for every EIS packet '
                'received during beaker text.'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    eis_record_id: Mapped[int] = mapped_column(Integer, nullable=False, comment='ID of the row in the eis_record table describing which GST and which measurement this data is for')
    frequency_idx: Mapped[int] = mapped_column(Integer, nullable=False, comment='Which frequency value')
    magnitude: Mapped[decimal.Decimal] = mapped_column(DECIMAL(10, 2), nullable=False, comment='Magnitude')
    phase: Mapped[decimal.Decimal] = mapped_column(DECIMAL(10, 2), nullable=False, comment='Phase')
    real_value: Mapped[int] = mapped_column(Integer, nullable=False, comment='Real value')
    imaginary_value: Mapped[int] = mapped_column(Integer, nullable=False, comment='Imaginary value')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')

    eis_record: Mapped['EisRecord'] = relationship('EisRecord', back_populates='eis_raw')


class TestedGst(Base):
    __tablename__ = 'tested_gst'
    __table_args__ = (
        ForeignKeyConstraint(['cgm_raw_id'], ['cgm_raw.id'], name='tested_gst_ibfk_2'),
        ForeignKeyConstraint(['eis_record_id'], ['eis_record.id'], name='tested_gst_ibfk_4'),
        ForeignKeyConstraint(['gst1_raw_id'], ['gst1_raw.id'], name='tested_gst_ibfk_3'),
        ForeignKeyConstraint(['gst_group_id'], ['gst_group.id'], name='tested_gst_ibfk_1'),
        ForeignKeyConstraint(['measurement_limits_id'], ['measurement_limits.id'], name='tested_gst_ibfk_5'),
        Index('cgm_raw_id', 'cgm_raw_id'),
        Index('eis_record_id', 'eis_record_id'),
        Index('group_index', 'group_index'),
        Index('gst1_raw_id', 'gst1_raw_id'),
        Index('gst_group_id', 'gst_group_id'),
        Index('measurement_limits_id', 'measurement_limits_id'),
        Index('serial', 'serial'),
        {'comment': 'Information on a GST that has finished plug testing.  When a new '
                'plug test is performed on this same GST, a new row in this table '
                'is created.'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    serial: Mapped[str] = mapped_column(String(64), nullable=False, comment='The serial # of the GST')
    group_index: Mapped[int] = mapped_column(Integer, nullable=False, comment='Index of this GST in the group')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    gst_group_id: Mapped[Optional[int]] = mapped_column(Integer, comment='ID of the row in the gst_group table describing the group in which this GST was plug tested.  Can be null if GST was deleted after plug testing completed.')
    cgm_raw_id: Mapped[Optional[int]] = mapped_column(Integer, comment='Row in the cgm_raw table containing the raw CGM data acquired during plug testing.  Null if data was not CGM format')
    gst1_raw_id: Mapped[Optional[int]] = mapped_column(Integer, comment='ID of the row in the gst1_raw table containing the raw GST1 data acquired during plug testing.  Null if data was not GST1 format')
    eis_record_id: Mapped[Optional[int]] = mapped_column(Integer, comment='ID of the row in the eis_record table allowing access to the raw EIS data acquired during plug testing.  Null if no EIS data was acquired')
    measurement_limits_id: Mapped[Optional[int]] = mapped_column(Integer, comment='ID of the row in the measurement_limits table allowing access to the limits used to determine pass/fail during plug testing.  Null if limits were not recorded')

    cgm_raw: Mapped[Optional['CgmRaw']] = relationship('CgmRaw', back_populates='tested_gst')
    eis_record: Mapped[Optional['EisRecord']] = relationship('EisRecord', back_populates='tested_gst')
    gst1_raw: Mapped[Optional['Gst1Raw']] = relationship('Gst1Raw', back_populates='tested_gst')
    gst_group: Mapped[Optional['GstGroup']] = relationship('GstGroup', back_populates='tested_gst')
    measurement_limits: Mapped[Optional['MeasurementLimits']] = relationship('MeasurementLimits', back_populates='tested_gst')
    sensor: Mapped[list['Sensor']] = relationship('Sensor', back_populates='tested_gst')


class Sensor(Base):
    __tablename__ = 'sensor'
    __table_args__ = (
        ForeignKeyConstraint(['beaker_id'], ['beaker.id'], name='sensor_ibfk_2'),
        ForeignKeyConstraint(['tested_gst_id'], ['tested_gst.id'], name='sensor_ibfk_1'),
        Index('beaker_id', 'beaker_id'),
        Index('tested_gst_id', 'tested_gst_id'),
        {'comment': 'Information on a sensor attached to a GST for beaker testing'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    tested_gst_id: Mapped[int] = mapped_column(Integer, nullable=False, comment='ID of the row in the tested_gst table describing the GST the sensor is connected to')
    trav_lot: Mapped[str] = mapped_column(String(200), nullable=False, comment='Traveler lot')
    passed_beaker_test: Mapped[str] = mapped_column(String(1), nullable=False, comment='Whether this sensor passed beaker test. P means pass, F means fail, - means test not performed or incomplete')
    hydration_delay_override: Mapped[int] = mapped_column(TINYINT, nullable=False, comment='True if user over-rode a hydration delay error')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    beaker_id: Mapped[Optional[int]] = mapped_column(Integer, comment='ID of the row in the beaker table describing the beaker this sensor was tested in.  Null if was removed from beaker.')
    plate: Mapped[Optional[int]] = mapped_column(Integer, comment='Plate number (null if not post-membrane test)')
    sensor_number: Mapped[Optional[int]] = mapped_column(Integer, comment='Sensor number (null if not post-membrane test)')
    dut_number: Mapped[Optional[int]] = mapped_column(Integer, comment='Device Under Test number (null if not post-sterilization test)')
    child_lot: Mapped[Optional[str]] = mapped_column(String(50), comment='Child lot (null unless MPROC)')
    session_start_time: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, comment='Session start time (null unless supported by collector)')
    error_lock_text: Mapped[Optional[str]] = mapped_column(String(200), comment='Text of the error (only non-null if sensor is error-locked)')
    error_lock_locus: Mapped[Optional[str]] = mapped_column(String(20), comment='Locus of the error (only non-null if sensor is error-locked)')

    beaker: Mapped[Optional['Beaker']] = relationship('Beaker', back_populates='sensor')
    tested_gst: Mapped['TestedGst'] = relationship('TestedGst', back_populates='sensor')
    sequence_sensor_result: Mapped[list['SequenceSensorResult']] = relationship('SequenceSensorResult', back_populates='sensor')


class SequenceSensorResult(Base):
    __tablename__ = 'sequence_sensor_result'
    __table_args__ = (
        ForeignKeyConstraint(['cgm_raw_id'], ['cgm_raw.id'], name='sequence_sensor_result_ibfk_5'),
        ForeignKeyConstraint(['eis_record_id'], ['eis_record.id'], name='sequence_sensor_result_ibfk_3'),
        ForeignKeyConstraint(['gst1_raw_id'], ['gst1_raw.id'], name='sequence_sensor_result_ibfk_4'),
        ForeignKeyConstraint(['sensor_id'], ['sensor.id'], name='sequence_sensor_result_ibfk_2'),
        ForeignKeyConstraint(['sequence_data_id'], ['sequence_data.id'], name='sequence_sensor_result_ibfk_1'),
        Index('cgm_raw_id', 'cgm_raw_id'),
        Index('eis_record_id', 'eis_record_id'),
        Index('gst1_raw_id', 'gst1_raw_id'),
        Index('sensor_id', 'sensor_id'),
        Index('sequence_data_id', 'sequence_data_id'),
        {'comment': 'Table containing information for individual GSTs in specific '
                'sequences'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment='ID of this row in the database')
    time_created: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP'), comment='When this database row was created')
    sequence_data_id: Mapped[int] = mapped_column(Integer, nullable=False, comment='ID of the row in the sequence_data table that describes the sequence this row contains data for')
    sensor_id: Mapped[int] = mapped_column(Integer, nullable=False, comment='ID of the row in the sensor table that describes the sensor this row contains data for')
    isig_pass: Mapped[int] = mapped_column(TINYINT, nullable=False, comment='Whether the ISig value for the specified GST at the end of the specified sequence was in range')
    time_last_modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='Time this row was last modified')
    eis_record_id: Mapped[Optional[int]] = mapped_column(Integer, comment='The EIS results for this sensor in this sequence (null if no EIS data)')
    gst1_raw_id: Mapped[Optional[int]] = mapped_column(Integer, comment='The GST1 raw data for this sensor in this sequence (null if data was not GST1 format)')
    cgm_raw_id: Mapped[Optional[int]] = mapped_column(Integer, comment='The GST1 raw data for this sensor in this sequence (null if data was not CGM format)')
    isig: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 2), comment='ISig value for the specified GST at the end of the specified sequence.  Null if no ISig data received for this GST in this sequence.')
    isig_roc: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 2), comment='Rate of change of ISig, in percent (0 to 100).  Null if rate of change not calculated (no RoC limits specified for this sequence)')
    isig_roc_pass: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Whether the ISig RoC was within specified limits')
    vcntr: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 3), comment='VCntr value for the specified GST at the end of the specified sequence')
    vcntr_pass: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Whether the VCntr value for the specified GST at the end of the specified sequence was in range')
    vcntr_roc: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(10, 2), comment='Rate of change of VCntr, in percent (0 to 100).  Null if rate of change not calculated (no RoC limits specified for this sequence)')
    vcntr_roc_pass: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Whether the VCntr RoC was within specified limits')
    eis_record_pass: Mapped[Optional[int]] = mapped_column(TINYINT, comment='Was EIS data within limits?')

    cgm_raw: Mapped[Optional['CgmRaw']] = relationship('CgmRaw', back_populates='sequence_sensor_result')
    eis_record: Mapped[Optional['EisRecord']] = relationship('EisRecord', back_populates='sequence_sensor_result')
    gst1_raw: Mapped[Optional['Gst1Raw']] = relationship('Gst1Raw', back_populates='sequence_sensor_result')
    sensor: Mapped['Sensor'] = relationship('Sensor', back_populates='sequence_sensor_result')
    sequence_data: Mapped['SequenceData'] = relationship('SequenceData', back_populates='sequence_sensor_result')
