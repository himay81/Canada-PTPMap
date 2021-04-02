# Field headers for input CSV file
# Dict keys are field headers, values represent whether they are imported or not.
names = {
    'TXRX': True,                       'Frequency': True,              'FrequencyRecordIdentifier': True,
    'RegulatoryService': False,         'CommunicationType': False,     'ConformityToFrequencyPlan': False,
    'FrequencyAllocationName': False,   'Channel': False,               'LegacySystemInternationalCoordinationNumber': False,
    'AnalogDigital': False,             'OccupiedBandwidthKHz': True,   'DesignationOfEmission': False,
    'ModulationType': False,            'FiltrationInstalled': False,   'TxERPdBW': False,
    'TxTransmitterPowerW': False,       'TotalLossesDB': False,         'AnalogCapacity': True,
    'DigitalCapacity': True,            'RxUnfadedSignalLevel': False,  'RxThresholdSignalLevelBer10e': False,
    'AntManufacturer': False,           'AntModel': False,              'AntGain': False,
    'AntPattern': False,                'HalfpowerBeamwidth': False,    'FrontToBackRatio': False,
    'Polarization': False,              'HeightAboveGroundLevel': True, 'AzimuthOfMainLobe': False,
    'VerticalElevationAngle': False,    'StationLocation': False,       'LicenseeStationReference': False,
    'Callsign': False,                  'StationType': False,           'ITUClassOfStation': False,
    'StationCostCategory': False,       'NumberOfIdenticalStations': False, 'ReferenceIdentifier': False,
    'Provinces': False,                 'Latitude': True,               'Longitude': True,
    'GroundElevationAboveSealevel': False, 'AntennaStructureHeightAboveGroundLevel': False, 'CongestionZone': False,
    'RadiusOfOperation': False,         'SatelliteName': False,         'AuthorizationNumber': True,
    'Service': True,                    'Subservice': True,             'LicenceType': False,
    'AuthorizationStatus': False,       'InserviceDate': True,          'AccountNumber': False,
    'LicenseeName': True,               'LicenseeAddress': False,       'OperationalStatus': False,
    'StationClassification': False,     'HorizontalPower': False,       'VerticalPower': False,
    'StandbyTransmitterInformation': False
}
