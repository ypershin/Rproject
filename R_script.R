library("RODBC")
library("plyr", lib.loc="C:/Program Files/R/R-3.0.3/library")
library("reshape", lib.loc="C:/Program Files/R/R-3.0.3/library")

# maximum sites in pms query
MAX_SITES = 350


args <- commandArgs(trailingOnly = TRUE)
# args <- c("110957","2014-07-22",7109,6171,7233,7048,7167,7232,8744,8246,7030,6412,5820,6501)

if(length(args) < 14) {
   stop("not enough arguments!!!")
}

amend <- as.numeric(args[3:14])

contractNumber <- args[1]
print(contractNumber)

# start time
print(Sys.time())

td <- as.POSIXlt(args[2])
firstMonth = as.Date(ISOdate(td$year + 1900 -1, td$mon+1, 1))
lastMonth = as.Date(ISOdate(td$year + 1900 +1, td$mon, 1))

priceDate = ""
filterString = ""
siteString = ""
startDate = ""
endDate = ""
pmsDetails = ""


replaceParameter <- function(pStr) {
  pStr <- gsub("\n", " ", pStr)
  pStr <- gsub("<CONTRACT_NUMBER>", contractNumber, pStr)
  pStr <- gsub("<PRICE_DATE>", priceDate, pStr)
  pStr <- gsub("<FIRST_MONTH>", firstMonth, pStr)
  pStr <- gsub("<LAST_MONTH>", lastMonth, pStr)
  pStr <- gsub("<START_DATE>", startDate, pStr)
  pStr <- gsub("<END_DATE>", endDate, pStr)
  pStr <- gsub("<SITE_STRING>", siteString, pStr)
  pStr <- gsub("<FILTER_STRING>", filterString, pStr)
  pStr <- gsub("<PMS_DETAILS>", pmsDetails, pStr)
  
  return(pStr)
} 

# function return Min or Max value in two dates
getDate <- function(prm, dt1, dt2) {
  # prm==1 for Max, prm==-1 for Min
  
  if(is.na(dt1))
    return(dt2)
  
  if(prm==1)
    return(max(dt1,dt2))
  else
    return(min(dt1,dt2))
}

# add quotes around site_id
addQuotes <- function(site) {
  site <- paste("'",site,sep="")
  return(paste(site,"'",sep=""))
}


dbAcc <- read.table("C:/Temp/dbAccounts.txt",header=T,sep="\t")


qrySites <- "select site_id, last_enrollment_date, last_deenrollment_date, contract_start_date,contract_end_date,
site_contract_eff_start_date, site_contract_eff_end_date, site_status
from contract_site_list 
where contract_num = '<CONTRACT_NUMBER>' order by site_contract_eff_start_date, site_contract_eff_end_date, site_id"

qryCustomerRep <- "select coalesce(customer_rep,'None') customer_rep from contract_list
where deal_status= 'Active' and contract_num = '<CONTRACT_NUMBER>'"

qryEec1 <- "select * from CONTRACT_POSITION_POWER 
where (srvc_no in (<SITE_STRING>) or (contract_no = '<CONTRACT_NUMBER>' and srvc_no is null)) 
and volume_month between to_date('<FIRST_MONTH>','yyyy-mm-dd') and to_date('<LAST_MONTH>','yyyy-mm-dd') 
order by srvc_no, volume_month"

qryEecN <- "select * from CONTRACT_POSITION_POWER 
where srvc_no in (<SITE_STRING>)
and volume_month between to_date('<FIRST_MONTH>','yyyy-mm-dd') and to_date('<LAST_MONTH>','yyyy-mm-dd') 
order by srvc_no, volume_month"


qryPrice = "select retail_price from comm_mkt_report.amendment_pricing 
  where end_date = to_date('<PRICE_DATE>','yyyy-mm-dd')"

qryPms <- "select site_id, trunc(wsd_date,'mon') monthly, sum(consumption_usage_amount) volume, wsd_flag from (
  <PMS_DETAILS>
)
where wsd_date between to_date('<FIRST_MONTH>','yyyy-mm-dd') and to_date('<LAST_MONTH>','yyyy-mm-dd')
group by site_id, trunc(wsd_date,'mon'), wsd_flag
order by site_id, trunc(wsd_date,'mon'), wsd_flag"


fStr <- "select site_id, wsd_date, consumption_usage_amount,
(case when wsd_date between to_date('<START_DATE>','yyyy-mm-dd') and to_date('<END_DATE>','yyyy-mm-dd') then 1 else 0 end) wsd_flag
from elec_position_owner.wsd_best_consumption
where site_id in (<SITE_STRING>)
"

# run queires in EECPRD

dsn <- "EECPRD"
userid <- as.character(dbAcc[dbAcc$DB==tolower(dsn),2])
passw <- as.character(dbAcc[dbAcc$DB==tolower(dsn),3])
conn <- odbcConnect(dsn,userid, passw, believeNRows=FALSE)
# odbcGetInfo(conn)

print("executing qryCustomerRep...")
custRep <- sqlQuery(conn, replaceParameter(qryCustomerRep))

print("executing qrySites...")
sites <- sqlQuery(conn, replaceParameter(qrySites))

# records in sites set
n <- nrow(sites)
print(n)

# add leading zeroes to site_id
if (nchar(sites$SITE_ID[1]) < 13) 
  sites$SITE_ID_LZ <- paste("00",sites$SITE_ID,sep="")


sites$SITE_TYPE <- "ORIGINAL"
sites$SITE_TYPE2 <- "NA"
sites$ORIGINAL <- TRUE

if(exists("recs"))
  rm(recs)

print("executing qryEec...")

# generate string of sites for qryEec
for(i in 1:n) {  
  if(i %% MAX_SITES == 1) 
    siteString = addQuotes(sites$SITE_ID_LZ[i])
  else 
    siteString = paste(siteString, addQuotes(sites$SITE_ID_LZ[i]),sep=",")
  
  if(i %% MAX_SITES == 0 | i==n) {
    print(i)
    # qryEec returns records from CONTRACT_POSITION_POWER for sites in a string of sites
    if(!exists("recs"))
      recs <- sqlQuery(conn, replaceParameter(qryEec1))
    else {
    # print(replaceParameter(qryEecN))
      tmp <- sqlQuery(conn, replaceParameter(qryEecN))
      recs <- rbind(recs, tmp)
      rm(tmp)      
    }
    # print(nrow(recs))
  }
  

  blnDrop = (sites$CONTRACT_END_DATE[i] != sites$SITE_CONTRACT_EFF_END_DATE[i])
  blnDeenroll = !is.na(sites$LAST_DEENROLLMENT_DATE[i])
  
  blnAdd = (sites$CONTRACT_START_DATE[i] != sites$SITE_CONTRACT_EFF_START_DATE[i])
  blnEnroll = !is.na(sites$LAST_ENROLLMENT_DATE[i])
  
  if( blnDrop) {
    sites$SITE_TYPE2[i] = "AMEND"
    if(blnDeenroll)
      sites$SITE_TYPE[i] = "DROP"    
  }  
  
  if( blnAdd) {
    sites$ORIGINAL[i] = FALSE
    if(blnEnroll)
      sites$SITE_TYPE[i] = "ADD"      
  }  
  
  if( blnDrop & blnAdd & blnDeenroll & blnEnroll) 
    sites$SITE_TYPE[i] = "ADD/DROP"   

}


# subset for this contract only
contr <- subset(recs,CONTRACT_NO==contractNumber)

# list of Summary values
sml <- with(contr, list(accMgr=as.character(custRep$CUSTOMER_REP[1]),legalName=as.character(CUSTOMER_NAME[1]),rateId=RATE_ID[1], 
                        contractStartDate=as.character(CONTRACT_START_DATE[1]), contractEndDate=as.character(DEAL_THRU_DATE[1]),
                        fixedPrice=BASED_PRICE[1],marketPrice=0.0, excessFee=EXCESS_ADMIN_FEE[1],
                        commodity="Electricity",ratePlan=as.character(RATE[1]),unusedFee=UNUSED_ADMIN_FEE[1],
                        upperThreshold=UPPER_THRESHOLD[1], lowerThreshold=LOWER_THRESHOLD[1],
                        financialVol=0.0,originalVol=0.0,forecastAaVol=0.0,amendVol=0.0,amendValueMarket=0.0,
                        amendValueFixed=0.0,amendValueGain=0.0,frcstVsFinPercent=0.0,actualVol=0.0,blnWithinThreshold=FALSE,
                        blnChangeMore500K=FALSE,blnPriceFavorable=FALSE, blnSendNotification=FALSE))


priceDate = getDate(-1,sml$contractEndDate,as.character(lastMonth))
print(priceDate)

print("executing qryPrice...")
price <- sqlQuery(conn, replaceParameter(qryPrice))

if(nrow(price)>0)
  sml$marketPrice = price[,1]/1000


odbcClose(conn)




# run PMS query

dsn <- "PMSPRD"
userid <- as.character(dbAcc[dbAcc$DB==tolower(dsn),2])
passw <- as.character(dbAcc[dbAcc$DB==tolower(dsn),3])

#dsn <- "PMSTST"
#userid <- as.character(dbAcc[dbAcc$DB==tolower(dsn),2])
#passw <- as.character(dbAcc[dbAcc$DB==tolower(dsn),3])


conn <- odbcConnect(dsn,userid, passw, believeNRows=FALSE)
# odbcGetInfo(conn)

if(exists("mv"))
  rm(mv)

print("executing qryPms...")

n = nrow(sites)

for(i in 1:n) {  
  if(i %% MAX_SITES == 1) {    
    filterString = ""
    siteString = addQuotes(sites$SITE_ID_LZ[i])
    startDate = getDate(1,sites$LAST_ENROLLMENT_DATE[i],sites$SITE_CONTRACT_EFF_START_DATE[i])
    endDate = getDate(-1,sites$LAST_DEENROLLMENT_DATE[i], sites$SITE_CONTRACT_EFF_END_DATE[i])    
  }
  else {
    if(sites$SITE_CONTRACT_EFF_START_DATE[i] == startDate  & sites$SITE_CONTRACT_EFF_END_DATE[i] == endDate) {
      siteString = paste(siteString,addQuotes(sites$SITE_ID_LZ[i]),sep=",")    
    }    
    else {
      filterString = paste(filterString, replaceParameter(fStr))
      filterString = paste(filterString,"union all")
      siteString = addQuotes(sites$SITE_ID_LZ[i])
      
      startDate = getDate(1,sites$LAST_ENROLLMENT_DATE[i],sites$SITE_CONTRACT_EFF_START_DATE[i])
      endDate = getDate(-1,sites$LAST_DEENROLLMENT_DATE[i], sites$SITE_CONTRACT_EFF_END_DATE[i])
    }
    if(i%%MAX_SITES==0 | i==n) {
      print(i)
      filterString <- paste(filterString, replaceParameter(fStr))
      pmsDetails <- paste(filterString, "union all")
      pmsDetails <- paste(pmsDetails, gsub("wsd_best_","wsd_final_", filterString))
      # print(replaceParameter(qryPms))
      if(!exists("mv")) 
        mv <- sqlQuery(conn, replaceParameter(qryPms))
      else {
        mv1 <- sqlQuery(conn, replaceParameter(qryPms))
        mv <- rbind(mv,mv1)
        rm(mv1)
      }
    }    
  }
}


# writeLines(replaceParameter(qryPms),file("C:/Temp/qry.txt"))


odbcClose(conn)

setSs <- function(df, strType) {  
  df[is.na(df)] <- 0
  df$TYPE <- strType
  names(df) <- c("SITE_ID","MONTH","VOLUME","TYPE")
  return(df)
}


# actual metered volumes for this contract
mvAct <- setSs(subset(mv,WSD_FLAG==1 & MONTHLY<ISOdate(td$year + 1900, td$mon, 1),select=c(SITE_ID,MONTHLY,VOLUME)),"actual")

# actual metered volumes for sites (regardless of contract)
mvAll <- setSs(subset(mv,TRUE,select=c(SITE_ID,MONTHLY,VOLUME)),"actualAC")

# historical volumes for sites (regardless of contract)
cpAll <- setSs(subset(recs, !is.na(SRVC_NO), select = c(SRVC_NO,VOLUME_MONTH,HISTORICAL_VOLUME)),"allHist")

# current physical volume
cp <- setSs(subset(recs, !is.na(SRVC_NO) & as.Date(VOLUME_MONTH)>=as.Date(sml$contractStartDate), select = c(SRVC_NO,VOLUME_MONTH,HISTORICAL_VOLUME)),"currPhys")

# original sites only
sitesOrig <- subset(sites,ORIGINAL==TRUE,select=c(SITE_ID))
names(sitesOrig) <- c("SRVC_NO")

# original physical volume
hv <- setSs(subset(merge(recs,sitesOrig,by="SRVC_NO"), CONTRACT_NO==contractNumber, select = c(SRVC_NO,VOLUME_MONTH,HISTORICAL_VOLUME)),"origPhys")

# financial volume
cf <- setSs(subset(recs, CONTRACT_NO==contractNumber, select = c(SRVC_NO,VOLUME_MONTH,CONTRACT_VOLUME)),"financial")

# combine sets into tv set
tv <- rbind(hv, cf)
tv <- rbind(tv, mvAct)
tv <- rbind(tv, mvAll)
tv <- rbind(tv, cp)

# pivot sets
cpOut <- cast(cpAll, SITE_ID ~ MONTH, fun.aggregate = sum, add.missing = TRUE, value = "VOLUME")
cvOut <- cast(cp, SITE_ID ~ MONTH, fun.aggregate = sum, add.missing = TRUE, value = "VOLUME")
hvOut <- cast(hv, SITE_ID ~ MONTH, fun.aggregate = sum, add.missing = TRUE, value = "VOLUME")
mvOut <- cast(mvAll, SITE_ID ~ MONTH, fun.aggregate = sum, add.missing = TRUE, value = "VOLUME")

# pivot tv set
out <- cast(tv, TYPE ~ MONTH, fun.aggregate = sum, add.missing = TRUE, value = "VOLUME")

# calculte upper threshold
cvUT <- out[out$TYPE=="financial",]
cvUT$TYPE <- "upperThres"
cvUT[,2:25] <- cvUT[,2:25]*sml$upperThreshold

# calculate lower threshold
cvLT <- out[out$TYPE=="financial",]
cvLT$TYPE <- "lowerThres"
cvLT[,2:25] <- cvLT[,2:25]*sml$lowerThreshold


# current physical volume row
fcstHuf <- out[out$TYPE=="currPhys",]
fcstHuf$TYPE <- "fcstHuf"
fcstHuf[,2:13] <- 0


# create actualBest row
actualBest <- out[out$TYPE=="actualAC",]
actualBest$TYPE <- "actualBest"

# actulBest[,15:25] calculation
siteActive <- subset(sites, SITE_STATUS=='Active', select=c(SITE_ID))

n = nrow(siteActive)
for(i in 1:n) {  
  smv <- subset(mvOut,SITE_ID==siteActive[i,1])
  shv <- subset(cpOut,SITE_ID==siteActive[i,1])
  
  for(m in 15:25) {    
    if(nrow(smv)>0) {
      if(smv[,m-12] != 0)
        actualBest[,m] = (actualBest[,m] + smv[,m-12])
      else
        actualBest[,m] = (actualBest[,m] + shv[,m])
    }
    else
      actualBest[,m] = (actualBest[,m] + shv[,m])    
  }
}


# forecast actual row
fcstAct <- fcstHuf
fcstAct$TYPE <- "fcstAct"
fcstAct[,15:25] <- actualBest[,15:25]

# amendment row
amd <- fcstHuf
amd$TYPE <- "currAmend"
amd[,2:13] <- 0
amd[,14:25] <- amend

# forecast actuals plus amendments
fcstAA <- fcstAct
fcstAA$TYPE <- "fcstAA"
fcstAA[,2:25] <- (fcstAct[,2:25] + amd[,2:25])


# upper threshold percent vaue
uThres <- fcstAct
uThres$TYPE <- "pctUpThres"
uThres[,2:14] <- sml$upperThreshold
uThres[,15:25] <- 0

# lower threshold percent vaue
lThres <- uThres
lThres$TYPE <- "pctLowThres"
lThres[,2:14] <- sml$lowerThreshold

# percentage Actual Huf
pctActHuf <- uThres
pctActHuf$TYPE <- "pctActHuf"

# percentage Actual AA
pctActAA <- uThres
pctActAA$TYPE <- "pctActAA"

# percentage Current AA
pctCurAA <- uThres
pctCurAA$TYPE <- "pctCurAA"


# calculation of percentages
for(m in 2:14) {
  smFin = sum(out[out$TYPE=="financial",][,m:(m+11)])
  sumAct = sum(out[out$TYPE=="actual",][,m:(m+11)])
  sumHuf = sum(fcstHuf[,m:(m+11)])
  sumFAct = sum(fcstAct[,m:(m+11)])
  sumFAA = sum(fcstAA[,m:(m+11)])
  
  pctActHuf[,m] = (sumAct+sumHuf)/smFin
  pctActAA[,m] = (sumAct+sumFAct)/smFin
  pctCurAA[,m] = (sumAct+sumFAA)/smFin
}

# add rows into out
out = rbind(out,cvUT)
out = rbind(out,cvLT)
out = rbind(out,fcstHuf)
out = rbind(out,fcstAct)
out = rbind(out,amd)
out = rbind(out,fcstAA)
out = rbind(out,actualBest)
out = rbind(out,uThres)
out = rbind(out,lThres)
out = rbind(out,pctActHuf)
out = rbind(out,pctActAA)
out = rbind(out,pctCurAA)


# calculate sml values
sml$originalVol = sum(out[out$TYPE=="origPhys",][,14:25])
sml$financialVol = sum(out[out$TYPE=="financial",][,14:25])
sml$actualVol = sum(out[out$TYPE=="actual",][,2:13])
sml$amendVol = sum(amend)
sml$forecastAaVol = sum(fcstAA[,14:25])
sml$amendValueMarket = with(sml, amendVol * marketPrice)
sml$amendValueFixed = with(sml, amendVol * fixedPrice)
sml$amendValueGain = with(sml,amendValueFixed - amendValueMarket)
sml$frcstVsFinPercent = with(sml, forecastAaVol / financialVol)
sml$blnWithinThreshold = with(sml, frcstVsFinPercent >= lowerThreshold & frcstVsFinPercent <= upperThreshold)
sml$blnChangeMore500K = sml$amendVol > 500000
sml$blnPriceFavorable = (sml$amendValueGain > 0)
sml$SendNotification = with(sml,!blnWithinThreshold | blnChangeMore500K)

# save csv files
path = "C:/Temp/"
write.csv(sites, file = paste(path, "sites.csv",sep=""), row.names = FALSE)
write.csv(price, file = paste(path, "price.csv",sep=""), row.names = FALSE)
write.csv(cvOut, file = paste(path, "out_CV.csv",sep=""), row.names = FALSE)
write.csv(hvOut, file = paste(path, "out_HV.csv",sep=""), row.names = FALSE)
write.csv(mvOut, file = paste(path, "out_MV.csv",sep=""), row.names = FALSE)
write.csv(out, file = paste(path, "out_Main.csv",sep=""), row.names = FALSE)
write.csv(t(data.frame(sml)), file = paste(path, "out_sml.csv",sep=""), row.names = TRUE)


# current amendment
curAmd <- data.frame(DATE_CHANGE="Current",SITE_TYPE="ADD",TOTAL=sml$amendVol)
if(sml$amendVol<0) 
  curAmd$SITE_TYPE = "DROP"  


# sites that have been changed
ss <- subset(sites,SITE_TYPE!="ORIGINAL",select=c(SITE_ID, SITE_TYPE, LAST_ENROLLMENT_DATE,LAST_DEENROLLMENT_DATE))


# move drop date to LAST_ENROLLMENT_DATE columnn

if(nrow(ss)>0) {
  for(i in 1:nrow(ss)) {
    if(ss$SITE_TYPE[i]=="DROP") 
      ss$LAST_ENROLLMENT_DATE[i]=ss$LAST_DEENROLLMENT_DATE[i]
  }
  ss <- subset(ss,TRUE,select=c(SITE_ID, SITE_TYPE, LAST_ENROLLMENT_DATE))
  names(ss) <- c("SITE_ID","SITE_TYPE","DATE_CHANGE")

  # calculate totals for changes (based on averages for ADD, taking last month volume for DROP)
  samd <- merge(ss,cpOut,by="SITE_ID")
  
  samd$TOTAL <- 0.0
  for(i in 1:nrow(samd)) {
    blnDrop = (samd$SITE_TYPE[i]=="DROP")
    cnt = 0
    for(m in 27:4) {
      if(samd[i,m]!=0 & cnt<12 & cnt>-1) {
        if(blnDrop) {
          samd$TOTAL[i] = samd[i,m]
          cnt = -1
        }    
        else {
          cnt = cnt + 1
          samd$TOTAL[i] = samd$TOTAL[i] + samd[i,m]
        }
      }
    }
    samd$TOTAL[i] = samd$TOTAL[i]/cnt*12
  }
  
  # aggregate changes by DATE_CHANGE and SITE_TYPE
  samdAgr <- aggregate(TOTAL ~ (DATE_CHANGE + SITE_TYPE), samd, sum)
  
  samdAgr$DATE_CHANGE <- as.character(samdAgr$DATE_CHANGE)
  curAmd <- rbind(curAmd, samdAgr)
}


# calculate ratio to financial volume
curAmd$PCT <- curAmd$TOTAL/sml$financialVol
write.csv(curAmd, file = paste(path, "out_amdSmry.csv",sep=""), row.names = FALSE)

# end time
print(Sys.time())
