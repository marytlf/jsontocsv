import sys
import simplejson as json
import os
import pandas as pd    


OC_Path_1=sys.argv[1]
#OC_Path_2=sys.argv[2]
OC_File_1=None
#OC_File_2=None
OC_ListOffers_1=[]
#OC_ListOffers_2=[]

def openOC(OCPath):
    try:
        FileResponse = open(OCPath,'r')
        return FileResponse
    except Exception as inst:   
        print (type(inst))
        print(inst.args)

def readOffers(OC_File,listOffers):
    joinList=""
    try:
        fileLines = OC_File.readlines()
        joinList = ''.join([str(item) for item in fileLines])
            #joinList += ''.join(item)
        
        jsonOC = json.loads(joinList)
        #print ("JSON >> "+str(jsonOC[1]["offerId"]))
        getAllOffers(jsonOC,listOffers)
    except Exception as inst:   
        print (type(inst))
        print(inst.args)

def getAllOffers(jsonOC,listOffers):
    try:
        for item in jsonOC:
            print ("JSON >> "+str(item["offerId"]))
            listOffers.append(item)

    except Exception as inst:   
        print (type(inst))
        print(inst.args)


def getDropdownValue(json_input):

    for att in json_input:
        if att["dropdownValue"] is not None:
            return True


def readAndCompareOffer(listOffers_1):
    
    df_th_list=[]
    file_name="out.csv"
    dataFrame= pd.DataFrame()
    for json_1 in listOffers_1:
        print(json_1["offerId"])
        #getDropdownValue(json_1["customAttributeValues"])
        if json_1["customAttributeValues"] is not None: 
            for att in json_1["customAttributeValues"]:
                if att["dropdownValue"] is not None:
                    att["definition"] = None
                    #print ("json antes >> "+str(json_1))
                    #json_1["customAttributeValues"]["dropdownValue"]["definition"]=None
                    #print ("json depois >> "+str(json_1))
        if json_1["entitlements"] is not None: 
            df = pd.json_normalize(json_1["entitlements"])
            print(df)
            df_ent = df._append(df, ignore_index=True)
            for ent in json_1["entitlements"]:
                if ent["entitlementBalanceImpacts"] is not None: 
                    df_balImp = pd.json_normalize(ent["entitlementBalanceImpacts"])
                    #print(df_balImp)
                    for balImpac in ent["entitlementBalanceImpacts"]:
                        df_balImp_aux = df_balImp._append(balImpac, ignore_index=True)

                        if balImpac["thresholdScheme"] is not None: 
                            df_th = pd.json_normalize(balImpac["thresholdScheme"])
                            thresholdScheme = balImpac["thresholdScheme"]
                            #df_th = pd.DataFrame(thresholdScheme)
                            #dataFrame._append(thresholdScheme, ignore_index=True)
                            for thr in balImpac["thresholdScheme"]["thresholds"]:
                                dfThr = pd.DataFrame(thresholdScheme["thresholds"])
                                df_th = df_th._append(dfThr,ignore_index=True)
                                for thrAct in thr["thresholdActions"]:
                                    thr_aux = pd.json_normalize(thrAct)
                                    df_th = df_th._append(thr_aux,ignore_index=True)
                
                print(dataFrame.columns)    
                dataFrame = dataFrame._append(df_th, ignore_index=True)
                            
                    
                                    
                            
                if ent["conditionFilters"] is not None: 
                    df_balCon = pd.json_normalize(ent["conditionFilters"])
                #print(df_balCon)
        
        df_ent.to_csv("entitlements.csv", sep=';', encoding='utf-8')
        df_balCon.to_csv("conditionFilters.csv", sep=';', encoding='utf-8')
        dataFrame.to_csv("thresholds.csv", sep=';', encoding='utf-8', index=False)
        df_balImp_aux.to_csv("entitlementBalanceImpacts.csv", sep=';', encoding='utf-8')


        df = pd.json_normalize(json_1)
        print(df)
        df.to_csv(file_name, sep=';', encoding='utf-8')

        



OC_File_1 = openOC(OC_Path_1)
#OC_File_2 = openOC(OC_Path_2)

readOffers(OC_File_1,OC_ListOffers_1)
#readOffers(OC_File_2,OC_ListOffers_2)


readAndCompareOffer(OC_ListOffers_1)