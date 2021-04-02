import re
import logging
import pandas as pd


class SiteLink:
    logging.basicConfig(filename="ptpmap-log.txt",
                        format='%(asctime)s\t%(levelname)s\t%(message)s',
                        level=logging.DEBUG)

    def __init__(self, rx, **kwargs):
        self.LicenseeName =     kwargs['LicenseeName']
        self.InserviceDate =    kwargs['InserviceDate']
        self.Frequency =        kwargs['Frequency']
        self.Bandwidth =        kwargs['OccupiedBandwidthKHz']
        self.TxLatitude =       kwargs['Latitude']
        self.TxLongitude =      kwargs['Longitude']
        self.TxAltitude =       kwargs['HeightAboveGroundLevel']
        self.AnalogCapacity =   kwargs['AnalogCapacity']
        self.DigitalCapacity =  kwargs['DigitalCapacity']
        # TODO Fix this up
        try:
            self.RxLatitude =   rx[rx['Frequency'] == self.Frequency].iloc[0]['Latitude']
            self.RxLongitude =  rx[rx['Frequency'] == self.Frequency].iloc[0]['Longitude']
            self.RxAltitude =   rx[rx['Frequency'] == self.Frequency].iloc[0]['HeightAboveGroundLevel']
            self.Link =         True
        except:
            self.RxLatitude =   rx.iloc[0]['Latitude']
            self.RxLongitude =  rx.iloc[0]['Longitude']
            self.RxAltitude =   rx.iloc[0]['HeightAboveGroundLevel']
            self.Link =         False
            logging.info("Flagging authorization {0} as the TX ({1}) and RX ({2}) frequencies mismatch".format(
                kwargs['AuthorizationNumber'], self.Frequency, rx.iloc[0]['Frequency']))

    def getTxLocation(self):
        return self.TxLatitude, self.TxLongitude, self.TxAltitude

    def getRxLocation(self):
        return self.RxLatitude, self.RxLongitude, self.RxAltitude


if __name__ == '__main__':
    pass
