#!/usr/bin/env python3

import pandas as pd
import progressbar as pg
import simplekml

# Field headers for input CSV file
names = [
    'TXRX', 'Frequency', 'FrequencyRecordIdentifier', 'RegulatoryService', 'CommunicationType',
    'ConformityToFrequencyPlan',
    'FrequencyAllocationName', 'Channel', 'LegacySystemInternationalCoordinationNumber', 'AnalogDigital',
    'OccupiedBandwidthKHz',
    'DesignationOfEmission', 'ModulationType', 'FiltrationInstalled', 'TxERPdBW', 'TxTransmitterPowerW',
    'TotalLossesDB', 'AnalogCapacity',
    'DigitalCapacity', 'RxUnfadedSignalLevel', 'RxThresholdSignalLevelBer10e', 'AntManufacturer', 'AntModel', 'AntGain',
    'AntPattern', 'HalfpowerBeamwidth',
    'FrontToBackRatio', 'Polarization', 'HeightAboveGroundLevel', 'AzimuthOfMainLobe', 'VerticalElevationAngle',
    'StationLocation', 'LicenseeStationReference',
    'Callsign', 'StationType', 'ITUClassOfStation', 'StationCostCategory', 'NumberOfIdenticalStations',
    'ReferenceIdentifier', 'Provinces', 'Latitude',
    'Longitude', 'GroundElevationAboveSealevel', 'AntennaStructureHeightAboveGroundLevel', 'CongestionZone',
    'RadiusOfOperation', 'SatelliteName',
    'AuthorizationNumber', 'Service', 'Subservice', 'LicenceType', 'AuthorizationStatus', 'InserviceDate',
    'AccountNumber', 'LicenseeName', 'LicenseeAddress',
    'OperationalStatus', 'StationClassification', 'HorizontalPower', 'VerticalPower', 'StandbyTransmitterInformation'
]

# Fields to skip on import; can be adjusted later on as necessary
skips = [
    'RegulatoryService', 'CommunicationType', 'ConformityToFrequencyPlan', 'FrequencyAllocationName', 'Channel',
    'LegacySystemInternationalCoordinationNumber', 'AnalogDigital', 'DesignationOfEmission', 'ModulationType',
    'FiltrationInstalled', 'TxERPdBW', 'TxTransmitterPowerW', 'TotalLossesDB', 'RxUnfadedSignalLevel',
    'RxThresholdSignalLevelBer10e', 'AntManufacturer', 'AntModel', 'AntGain', 'AntPattern', 'HalfpowerBeamwidth',
    'FrontToBackRatio', 'Polarization', 'AzimuthOfMainLobe', 'VerticalElevationAngle', 'StationLocation',
    'LicenseeStationReference', 'Callsign', 'StationType', 'ITUClassOfStation', 'StationCostCategory',
    'NumberOfIdenticalStations', 'ReferenceIdentifier', 'Provinces', 'GroundElevationAboveSealevel',
    'AntennaStructureHeightAboveGroundLevel', 'CongestionZone', 'RadiusOfOperation', 'SatelliteName', 'LicenceType',
    'AuthorizationStatus', 'AccountNumber', 'LicenseeAddress', 'OperationalStatus', 'StationClassification',
    'HorizontalPower', 'VerticalPower', 'StandbyTransmitterInformation'
]

# Load all of the CSV while skipping unneeded fields
csvd = pd.read_csv('TAFL_LTAF.csv', names=names, usecols=[n for n in names if n not in skips])
# Temporarily drop locations with OccupiedBandwidthKHz = 0 as they appear to be relay points
csvd = csvd.drop(csvd[csvd['OccupiedBandwidthKHz'] == 0].index)
# zip() latitude and longitude together to ease identifying paired links
csvd['LatLong'] = list(zip(csvd['Latitude'], csvd['Longitude']))
# Slice out all TX and RX records for Service=(2), Subservice=(200)
txRecords = csvd[csvd['Service'] == 2][csvd['Subservice'].isin([200])][csvd['TXRX'] == 'TX']
rxRecords = csvd[csvd['Service'] == 2][csvd['Subservice'].isin([200])][csvd['TXRX'] == 'RX']
# Uncomment this block for a separate set of mP2P (Subservice=(201)) locations
# txRec201 = csvd[csvd['Service'] == 2][csvd['Subservice'].isin([201])][csvd['TXRX'] == 'TX']
# rxRec201 = csvd[csvd['Service'] == 2][csvd['Subservice'].isin([201])][csvd['TXRX'] == 'RX']

print("Found {0} TX licenses and {1} RX licenses".format(txRecords.shape[0], rxRecords.shape[0]))
# Get a list of all the unique AuthorizationNumbers to iterate through
txLicAuthNumSet = set(txRecords['AuthorizationNumber'])

for idx, row in txRecords.iterrows():
    rxIdx = rxRecords[rxRecords['AuthorizationNumber'] == row['AuthorizationNumber']][
        rxRecords['Frequency'] == row['Frequency']].index[0]
    print("TX index {0} matches RX index {1}".format(idx, rxIdx))

for txRecord in progressbar.progressbar(txRecords):
    link = {'tx': txRecord}
    rxRecords = []
    for txlicense in cleanrx:
        if txlicense['AuthorizationNumber'] == txRecord['AuthorizationNumber'] and \
                txlicense['Frequency'] == txRecord['Frequency'] and \
                txlicense['Latitude'] != txRecord['Latitude'] and \
                txlicense['Longitude'] != txRecord['Longitude']:
            rxRecords.append(txlicense)
    link['rx'] = rxRecords
    ptpLinks.append(link)

print("Finding RX licenses done, starting KML generation")

kml = simplekml.Kml()

bellStyle = simplekml.Style()
bellStyle.linestyle.width = 2
bellStyle.linestyle.color = 'ffff0000'  # Blue

rogersStyle = simplekml.Style()
rogersStyle.linestyle.width = 2
rogersStyle.linestyle.color = 'ff0000ff'  # Red

telusStyle = simplekml.Style()
telusStyle.linestyle.width = 2
telusStyle.linestyle.color = 'ff3CFF14'  # Green

xplornetStyle = simplekml.Style()
xplornetStyle.linestyle.width = 2
xplornetStyle.linestyle.color = 'FF1478A0'  # Brown

freedomStyle = simplekml.Style()
freedomStyle.linestyle.width = 2
freedomStyle.linestyle.color = 'ff14B4FF'  # Orange

otherStyle = simplekml.Style()
otherStyle.linestyle.width = 2
otherStyle.linestyle.color = 'ffFF78F0'  # Magenta


def styleLink(licName, kmlLink):
    kmlLink.altitudemode = simplekml.AltitudeMode.relativetoground
    licNameLow = licName.lower()

    if licNameLow.find('bell') != -1:
        kmlLink.style = bellStyle
    elif licNameLow.find('rogers') != -1:
        kmlLink.style = rogersStyle
    elif licNameLow.find('telus') != -1:
        kmlLink.style = telusStyle
    elif licNameLow.find('xplornet') != -1:
        kmlLink.style = xplornetStyle
    elif licNameLow.find('freedom mobile') != -1:
        kmlLink.style = freedomStyle
    else:
        kmlLink.style = otherStyle


for ptp in progressbar.progressbar(ptpLinks):
    if len(ptp['rx']) == 1:

        linkDesc = f"""
        Bandwidth(MHz): {str(float(ptp['tx']['OccupiedBandwidthKHz']) / 1000)}
        Analog Capacity (Calls): {str(ptp['tx']['AnalogCapacity'])}
        Digital Capacity (Mbps): {str(ptp['tx']['DigitalCapacity'])}
        In Service Date: {str(ptp['tx']['InserviceDate'])}
        """

        kmlLink = kml.newlinestring(
            name="{} | {}".format(ptp['tx']['LicenseeName'], str(ptp['tx']['Frequency'])),
            description=linkDesc,
            coords=[
                (ptp['tx']['Longitude'], ptp['tx']['Latitude'], ptp['tx']['HeightAboveGroundLevel']),
                (ptp['rx'][0]['Longitude'], ptp['rx'][0]['Latitude'], ptp['rx'][0]['HeightAboveGroundLevel']),
            ]

        )
        styleLink(ptp['tx']['LicenseeName'], kmlLink)

    elif len(ptp['rx']) > 1:
        # Subservice 201 is Point to Multipoint, one TX with multiple RX
        # There's not really a good way to display this for large systems like BC Hydro or Milton Hydro
        # It just looks like a mess and makes Google Earth chug, so I'm whitelisting a few interesting systems
        if ptp['tx']['Subservice'] == "201" and ptp['tx']['LicenseeName'] in [
            'Bell Canada', 'Northwestel Inc.', 'Telus Communications Inc.', 'Sasktel', 'Hydro-Québec'
        ]:
            for endpoint in ptp['rx']:
                kmlLink = kml.newlinestring(
                    name="{} | {}".format(ptp['tx']['LicenseeName'], str(ptp['tx']['Frequency'])),
                    description=linkDesc,
                    coords=[
                        (ptp['tx']['Longitude'], ptp['tx']['Latitude'], ptp['tx']['HeightAboveGroundLevel']),
                        (endpoint['Longitude'], endpoint['Latitude'], endpoint['HeightAboveGroundLevel']),
                    ]

                )
                styleLink(ptp['tx']['LicenseeName'], kmlLink)
        else:

            errorMsg = f"""
Multiple RX Frequencies Found!
DBID: {ptp['tx']['FrequencyRecordIdentifier']}
TXFrequency: {ptp['tx']['Frequency']}
LicenseeName: {ptp['tx']['LicenseeName']}
AuthorizationNumber: {ptp['tx']['AuthorizationNumber']}
--------------------------\n
            """
            # print(errorMsg)
            logFile.write(errorMsg)

    elif len(ptp['rx']) == 0:

        errorMsg = f"""
No RX frequencies found!
DBID: {ptp['tx']['FrequencyRecordIdentifier']}
TXFrequency: {ptp['tx']['Frequency']}
LicenseeName: {ptp['tx']['LicenseeName']}
AuthorizationNumber: {ptp['tx']['AuthorizationNumber']}
--------------------------\n
        """
        # print(errorMsg)
        logFile.write(errorMsg)

print("Saving KML and Log")
kml.save("ptpmap.kml")
logFile.close()
