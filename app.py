import initialiser as intlsr

if __name__ == '__main__':
    initialiser = intlsr.Initialiser()
    initialiser.startRefineryInfoFranchisingBrokerReceiving()
    initialiser.startRystadWatcher()   
    initialiser.startMcQuillingWatcher()     
    initialiser.startIeaWatchers()
    initialiser.startClipperFloatingStorageWatcher()     
    initialiser.startEIAUpdateBrokerReceiving() 
    initialiser.startEiaImportScheduler()
    initialiser.startMcQuillingImportScheduler()

