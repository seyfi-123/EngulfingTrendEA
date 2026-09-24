//+------------------------------------------------------------------+
//| EngulfingTrendEA.mq5                                              |
//| Strategiya: Trend bo'yicha Engulfing (yutuvchi svecha) signali    |
//| SL: yutilgan svechaning tag/tepasida                              |
//| TP: ATR asosida avtomatik 1:2 yoki 1:3 RR                         |
//| Lot: balansdan % risk asosida hisoblanadi                         |
//| Xotira: yopilgan savdolar faylga yoziladi, ketma-ket zararlardan  |
//|          keyin avtomatik pauza (himoya mexanizmi)                 |
//| Version 2.00 - Filterlar qo'shildi: ADX, ATR, EMA sep, News,     |
//|                Rollover (flat bozor himoyasi)                    |
//+------------------------------------------------------------------+
#property copyright "Custom EA"
#property version   "2.00"
#property strict

#include <Trade\Trade.mqh>
CTrade trade;

// Forward declarations
void OpenBuy(double entry, double rr, bool isCounterTrend);
void OpenSell(double entry, double rr, bool isCounterTrend);
void SaveNewPositionRisk(double slDist, bool strongTrend);
void RunHistoricalBreakoutScan();
bool IsNewsTime();
bool IsRolloverTime();
bool IsFlatMarket();

//================== INPUTLAR ==================
input group "=== Risk / Lot sozlamalari ==="
input bool   InpUseRiskPercent   = true;   // true=% risk bo'yicha lot, false=fiksirlangan lot
input double InpRiskPercent      = 2.0;    // Har savdoga risk (% balansdan)
input double InpFixedLot         = 0.02;   // Fiksirlangan/zaxira lot
input double InpMinLot           = 0.02;   // Minimal ruxsat etilgan lot

input group "=== Trend filtri ==="
input int    InpFastMA           = 50;     // Tez EMA davri
input int    InpSlowMA           = 200;    // Sekin EMA davri
input ENUM_MA_METHOD InpMAMethod = MODE_EMA;

input group "=== Engulfing / ATR sozlamalari ==="
input int    InpATRPeriod        = 14;     // ATR davri
input double InpATRMultiplier    = 1.5;    // Kuchli svecha chegarasi (ATR ko'paytmasi)
input double InpSLBufferPoints   = 20;     // SL uchun qo'shimcha bufer (points)

input group "=== FLAT BOZOR HIMOYASI (YANGI) ==="
input bool   InpUseADXFilter      = true;   // ADX filtri (flat bozorni aniqlaydi)
input int    InpADXPeriod         = 14;     // ADX davri
input double InpADXMin            = 20.0;   // ADX < 20 → savdo YO'Q (flat)
input bool   InpUseATRAverageFilter = true; // ATR o'rtacha filtri (sekin bozor)
input int    InpATRAvgPeriod      = 100;    // ATR o'rtacha davri
input double InpATRMinRatio       = 0.5;    // Hozirgi ATR < 0.5 * o'rtacha → savdo YO'Q
input bool   InpUseEMASeparation  = true;   // EMA'lar yetarlicha ajralganmi
input double InpEMASepRatio       = 0.8;    // |FastMA-SlowMA| < 0.8 * ATR → savdo YO'Q

input group "=== VAQT FILTRLARI (YANGI) ==="
input bool   InpUseNewsFilter     = true;   // Iqtisodiy xabarlar filtri (yuqori ahamiyatli)
input int    InpNewsBufferMin     = 30;     // Xabardan oldin/keyin bufer (daqiqa)
input bool   InpUseRolloverFilter = true;   // Rollover vaqti (spread kengayishi)

input group "=== Counter-Trend (faqat kichik TF: M5/M15) ==="
input bool   InpAllowCounterTrendLTF     = true;  // M5/M15'da trendga qarshi savdoga ruxsat
input double InpCounterTrendATRMultiplier= 2.2;   // Counter-trend uchun talab qilinadigan kuch (kattaroq bo'lishi kerak)
input double InpCounterTrendRiskMultiplier=0.5;   // Counter-trend savdolarda riskni kamaytirish

input group "=== Xavfsizlik / Xotira ==="
input int    InpMagicNumber      = 20260924;
input double InpMaxSpreadPoints  = 30;     // Maks. ruxsat etilgan spread
input int    InpMaxConsecLosses  = 3;      // Shu qadar ketma-ket zarardan keyin pauza
input int    InpPauseHours       = 24;     // Pauza davomiyligi (soat)
input bool   InpOnlyOnePosition  = true;   // Bir vaqtda faqat 1 pozitsiya

input group "=== Professional: Ijro va Xato boshqaruvi ==="
input int    InpSlippagePoints   = 15;     // Ruxsat etilgan slippage (deviation)
input int    InpMaxRetries       = 3;      // Order xatoligida qayta urinishlar soni
input int    InpRetryDelayMs     = 500;    // Qayta urinishlar orasidagi kutish (ms)

input group "=== Professional: Kunlik zarar limiti ==="
input bool   InpUseDailyLossLimit= true;
input double InpMaxDailyLossPct  = 5.0;    // Kun boshidagi balansdan max ruxsat etilgan zarar %

input group "=== Professional: Sessiya filtri ==="
input bool   InpUseSessionFilter = false;  // Faqat belgilangan soatlarda savdo qilish
input int    InpSessionStartHour = 0;      // Server vaqti bo'yicha boshlanish soati
input int    InpSessionEndHour   = 23;     // Server vaqti bo'yicha tugash soati

input group "=== Professional: Break-Even / Dinamik R-zinapoya ==="
input bool   InpUseBreakEven          = true;  // Trailing/breakeven tizimini yoqish
input double InpBreakEvenRR           = 1.0;   // 1-bosqich: shu R'da SL entry'ga (breakeven)
input double InpLadderTrigger2R       = 3.0;   // 2-bosqich (faqat kuchli trendda): shu R'da SL 2R'ga qulflanadi
input double InpLadderTrigger3R       = 3.5;   // 3-bosqich (faqat kuchli trendda): shu R'da SL 3R'ga qulflanadi
input double InpLadderMaxR            = 5.0;   // Maksimal maqsad R (kuchli trendda TP shu yerga qo'yiladi)
input double InpBreakEvenBufferPoints = 10;    // Bufer (spread/komissiya qoplash uchun)
input double InpStrongTrendATRRatio   = 2.0;   // |FastMA-SlowMA|/ATR shundan katta bo'lsa "kuchli trend"

input group "=== Professional: Tahlil va Kunlik Hisobot (Tojik tilida) ==="
input bool   InpPrintDailyReportTajik = true;  // Har kun tugaganda hisobot chiqarish
input bool   InpSaveDailyReportToFile = true;  // Hisobotni faylga ham saqlash

input group "=== Tebranish (Swing) / Breakout / W-M Tahlili ==="
input int    InpSwingLookback     = 50;    // Min/Max (swing) qidiruv oynasi (bar)
input double InpSwingTolerancePctATR = 0.5;// W/M model uchun ruxsat etilgan farq (ATR ko'paytmasi)
input bool   InpRunHistoricalScan = false; // EA birinchi ishga tushganda tarixiy skanerlash (SEKIN!)
input int    InpHistScanBars      = 20000; // Skanerlanadigan max bar soni
input int    InpHistForwardBars   = 10;    // Breakoutdan keyin necha bar tekshiriladi
input double InpHistContinueATR   = 1.0;   // "Davom etdi" deb hisoblash uchun ATR ko'paytmasi
input double InpStrongBodyRatio   = 0.6;   // Kuchli svecha: body/range shundan katta
input double InpStrongClosePos    = 0.7;   // Kuchli svecha: yopilish nuqtasi ekstremumga yaqin

input group "=== Professional: Yuqori TF trend tasdig'i ==="
input bool   InpUseHigherTFConfirm = true;   // Yuqori taymfreym trendini ham tekshirish

input group "=== Professional: Statistikaga asoslangan risk ==="
input bool   InpUseConfidenceScaling       = true;
input int    InpMinSampleForConfidence     = 20;
input double InpConfidenceWinRateThreshold = 70.0;
input double InpMaxConfidenceMultiplier    = 1.3;
input bool   InpReduceRiskAfterLoss        = true;
input double InpPostLossRiskMultiplier     = 0.5;
input double InpPostWinRiskMultiplier      = 1.2;
input double InpAbsoluteMaxRiskPercent     = 4.0;

input group "=== Professional: Multi-Timeframe Trend Tasdig'i ==="
input bool   InpUseMTFConfirmation = true;          // Yuqori TF'lar trend bilan rozimi tekshirish
input ENUM_TIMEFRAMES InpHigherTF1 = PERIOD_H1;     // 1-yuqori timeframe
input ENUM_TIMEFRAMES InpHigherTF2 = PERIOD_H4;     // 2-yuqori timeframe

//================== GLOBAL O'ZGARUVCHILAR ==================
int    handleFastMA, handleSlowMA, handleATR, handleADX;
int    handleHigherFastMA=INVALID_HANDLE, handleHigherSlowMA=INVALID_HANDLE;
ENUM_TIMEFRAMES higherTF = PERIOD_CURRENT;
string gvConsecLosses, gvPauseUntil, gvLastTicket;
string gvDayStartBalance, gvDayStartDate, gvDailyPaused;
string gvDayTrades, gvDayWins, gvDayLosses, gvDayProfitSum;
string statsKey;
string logFileName, reportFileName;
string gvHistDone, gvHistStrongWin, gvHistStrongTotal, gvHistWeakWin, gvHistWeakTotal;
string gvLastResult;

//+------------------------------------------------------------------+
int OnInit()
{
   handleFastMA = iMA(_Symbol, _Period, InpFastMA, 0, InpMAMethod, PRICE_CLOSE);
   handleSlowMA = iMA(_Symbol, _Period, InpSlowMA, 0, InpMAMethod, PRICE_CLOSE);
   handleATR    = iATR(_Symbol, _Period, InpATRPeriod);
   handleADX    = iADX(_Symbol, _Period, InpADXPeriod);

   if(handleFastMA==INVALID_HANDLE || handleSlowMA==INVALID_HANDLE ||
      handleATR==INVALID_HANDLE || handleADX==INVALID_HANDLE)
   {
      Print("Indikator handle yaratishda xatolik!");
      return(INIT_FAILED);
   }

   trade.SetExpertMagicNumber(InpMagicNumber);
   trade.SetDeviationInPoints(InpSlippagePoints);

   // Yuqori taymfreymni avtomatik aniqlash
   if(InpUseHigherTFConfirm)
   {
      if(_Period==PERIOD_M1 || _Period==PERIOD_M3 || _Period==PERIOD_M5)      higherTF = PERIOD_M30;
      else if(_Period==PERIOD_M15 || _Period==PERIOD_M30)                    higherTF = PERIOD_H1;
      else if(_Period==PERIOD_H1)                                            higherTF = PERIOD_H4;
      else if(_Period==PERIOD_H4)                                            higherTF = PERIOD_D1;
      else                                                                   higherTF = PERIOD_CURRENT;

      if(higherTF != PERIOD_CURRENT)
      {
         handleHigherFastMA = iMA(_Symbol, higherTF, InpFastMA, 0, InpMAMethod, PRICE_CLOSE);
         handleHigherSlowMA = iMA(_Symbol, higherTF, InpSlowMA, 0, InpMAMethod, PRICE_CLOSE);
      }
   }

   string key = _Symbol + "_" + IntegerToString(_Period);
   gvConsecLosses    = "EA_ConsecLosses_" + key;
   gvPauseUntil      = "EA_PauseUntil_" + key;
   gvLastTicket      = "EA_LastTicket_" + key;
   gvDayStartBalance = "EA_DayStartBal_" + key;
   gvDayStartDate    = "EA_DayStartDate_" + key;
   gvDailyPaused     = "EA_DailyPaused_" + key;

   if(!GlobalVariableCheck(gvConsecLosses)) GlobalVariableSet(gvConsecLosses, 0);
   if(!GlobalVariableCheck(gvPauseUntil))   GlobalVariableSet(gvPauseUntil, 0);
   if(!GlobalVariableCheck(gvLastTicket))   GlobalVariableSet(gvLastTicket, 0);
   if(!GlobalVariableCheck(gvDayStartBalance)) GlobalVariableSet(gvDayStartBalance, AccountInfoDouble(ACCOUNT_BALANCE));
   if(!GlobalVariableCheck(gvDayStartDate))    GlobalVariableSet(gvDayStartDate, 0);
   if(!GlobalVariableCheck(gvDailyPaused))     GlobalVariableSet(gvDailyPaused, 0);

   statsKey = key;
   gvDayTrades    = "EA_DayTrades_" + key;
   gvDayWins      = "EA_DayWins_" + key;
   gvDayLosses    = "EA_DayLosses_" + key;
   gvDayProfitSum = "EA_DayProfitSum_" + key;
   if(!GlobalVariableCheck(gvDayTrades))    GlobalVariableSet(gvDayTrades, 0);
   if(!GlobalVariableCheck(gvDayWins))      GlobalVariableSet(gvDayWins, 0);
   if(!GlobalVariableCheck(gvDayLosses))    GlobalVariableSet(gvDayLosses, 0);
   if(!GlobalVariableCheck(gvDayProfitSum)) GlobalVariableSet(gvDayProfitSum, 0);

   logFileName    = "EngulfingEA_Log_" + key + ".csv";
   reportFileName = "EngulfingEA_DailyReport_Tajik_" + key + ".txt";

   gvHistDone        = "EA_HistDone_" + key;
   gvHistStrongWin   = "EA_HistStrongWin_" + key;
   gvHistStrongTotal = "EA_HistStrongTotal_" + key;
   gvHistWeakWin     = "EA_HistWeakWin_" + key;
   gvHistWeakTotal   = "EA_HistWeakTotal_" + key;
   if(!GlobalVariableCheck(gvHistDone)) GlobalVariableSet(gvHistDone, 0);

   gvLastResult = "EA_LastResult_" + key;
   if(!GlobalVariableCheck(gvLastResult)) GlobalVariableSet(gvLastResult, -1);

   if(InpRunHistoricalScan && GlobalVariableGet(gvHistDone) < 1)
      RunHistoricalBreakoutScan();

   Print("EngulfingTrendEA v2.00 ishga tushdi. Flat bozor himoyasi: ADX=", InpUseADXFilter,
         " ATR-Avg=", InpUseATRAverageFilter, " EMA-Sep=", InpUseEMASeparation,
         " News=", InpUseNewsFilter, " Rollover=", InpUseRolloverFilter);

   return(INIT_SUCCEEDED);
}

void OnDeinit(const int reason)
{
   IndicatorRelease(handleFastMA);
   IndicatorRelease(handleSlowMA);
   IndicatorRelease(handleATR);
   IndicatorRelease(handleADX);
   if(handleHigherFastMA!=INVALID_HANDLE) IndicatorRelease(handleHigherFastMA);
   if(handleHigherSlowMA!=INVALID_HANDLE) IndicatorRelease(handleHigherSlowMA);
   Comment("");
}

//+------------------------------------------------------------------+
//| Trendni aniqlash: 1=Up, -1=Down, 0=Range                          |
//+------------------------------------------------------------------+
int DetectTrend()
{
   double fastMA[], slowMA[];
   ArraySetAsSeries(fastMA, true);
   ArraySetAsSeries(slowMA, true);

   if(CopyBuffer(handleFastMA, 0, 1, 3, fastMA) < 3) return 0;
   if(CopyBuffer(handleSlowMA, 0, 1, 3, slowMA) < 3) return 0;

   double close1 = iClose(_Symbol, _Period, 1);

   if(fastMA[0] > slowMA[0] && close1 > fastMA[0])
      return 1;
   if(fastMA[0] < slowMA[0] && close1 < fastMA[0])
      return -1;

   return 0;
}

//+------------------------------------------------------------------+
//| YANGI: Flat bozor filtri (ADX + ATR-avg + EMA sep)                |
//+------------------------------------------------------------------+
bool IsFlatMarket()
{
   // --- 1. ADX filtri ---
   if(InpUseADXFilter)
   {
      double adx[];
      ArraySetAsSeries(adx, true);
      if(CopyBuffer(handleADX, 0, 1, 1, adx) > 0)
      {
         if(adx[0] < InpADXMin)
         {
            static datetime lastLog = 0;
            if(TimeCurrent() - lastLog > 3600)
            {
               Print("[FLAT FILTER] ADX=", DoubleToString(adx[0],1), " < ", InpADXMin, " — savdo yo'q (flat bozor)");
               lastLog = TimeCurrent();
            }
            return true;
         }
      }
   }

   // --- 2. ATR o'rtacha filtri ---
   if(InpUseATRAverageFilter)
   {
      double atrNow[], atrAvg[];
      ArraySetAsSeries(atrNow, true);
      ArraySetAsSeries(atrAvg, true);
      if(CopyBuffer(handleATR, 0, 1, 1, atrNow) > 0 &&
         CopyBuffer(handleATR, 0, 1, InpATRAvgPeriod, atrAvg) > 0)
      {
         double avg = 0;
         int n = ArraySize(atrAvg);
         for(int i=0; i<n; i++) avg += atrAvg[i];
         avg /= n;

         if(avg > 0 && atrNow[0] < avg * InpATRMinRatio)
         {
            static datetime lastLog2 = 0;
            if(TimeCurrent() - lastLog2 > 3600)
            {
               Print("[FLAT FILTER] ATR=", DoubleToString(atrNow[0],5), " < ",
                     DoubleToString(avg * InpATRMinRatio,5), " — sekin bozor, savdo yo'q");
               lastLog2 = TimeCurrent();
            }
            return true;
         }
      }
   }

   // --- 3. EMA separation filtri ---
   if(InpUseEMASeparation)
   {
      double fastMA[], slowMA[], atr[];
      ArraySetAsSeries(fastMA, true);
      ArraySetAsSeries(slowMA, true);
      ArraySetAsSeries(atr, true);
      if(CopyBuffer(handleFastMA, 0, 1, 1, fastMA) > 0 &&
         CopyBuffer(handleSlowMA, 0, 1, 1, slowMA) > 0 &&
         CopyBuffer(handleATR, 0, 1, 1, atr) > 0)
      {
         double sep = MathAbs(fastMA[0] - slowMA[0]);
         if(sep < atr[0] * InpEMASepRatio)
         {
            static datetime lastLog3 = 0;
            if(TimeCurrent() - lastLog3 > 3600)
            {
               Print("[FLAT FILTER] EMA separation=", DoubleToString(sep,5),
                     " < ", DoubleToString(atr[0] * InpEMASepRatio,5), " — EMA'lar yaqin, savdo yo'q");
               lastLog3 = TimeCurrent();
            }
            return true;
         }
      }
   }

   return false;
}

//+------------------------------------------------------------------+
//| YANGI: Xabar vaqti filtri (yuqori ahamiyatli xabarlar)            |
//+------------------------------------------------------------------+
bool IsNewsTime()
{
   if(!InpUseNewsFilter) return false;

   MqlCalendarValue values[];
   datetime from = TimeCurrent() - InpNewsBufferMin * 60;
   datetime to   = TimeCurrent() + InpNewsBufferMin * 60;

   if(!CalendarValueHistory(values, from, to, NULL, NULL))
      return false;

   for(int i=0; i<ArraySize(values); i++)
   {
      MqlCalendarEvent event;
      if(CalendarEventById(values[i].event_id, event))
      {
         if(event.importance == CALENDAR_IMPORTANCE_HIGH)
         {
            static datetime lastNewsLog = 0;
            if(TimeCurrent() - lastNewsLog > 1800)
            {
               Print("[NEWS FILTER] Yuqori ahamiyatli xabar: ", event.name,
                     " (", TimeToString(values[i].time, TIME_DATE|TIME_MINUTES), ")");
               lastNewsLog = TimeCurrent();
            }
            return true;
         }
      }
   }
   return false;
}

//+------------------------------------------------------------------+
//| YANGI: Rollover vaqti filtri (23:55 - 00:05)                      |
//+------------------------------------------------------------------+
bool IsRolloverTime()
{
   if(!InpUseRolloverFilter) return false;

   MqlDateTime tm;
   TimeToStruct(TimeCurrent(), tm);

   if((tm.hour == 23 && tm.min >= 55) || (tm.hour == 0 && tm.min <= 5))
   {
      static datetime lastRollLog = 0;
      if(TimeCurrent() - lastRollLog > 1800)
      {
         Print("[ROLLOVER FILTER] Rollover vaqti — spread kengayishi mumkin, savdo yo'q");
         lastRollLog = TimeCurrent();
      }
      return true;
   }
   return false;
}

//+------------------------------------------------------------------+
//| Bullish / Bearish Engulfing                                       |
//+------------------------------------------------------------------+
bool IsBullishEngulfing()
{
   double open1 = iOpen(_Symbol,_Period,1), close1 = iClose(_Symbol,_Period,1);
   double open2 = iOpen(_Symbol,_Period,2), close2 = iClose(_Symbol,_Period,2);

   bool bearish2 = close2 < open2;
   bool bullish1 = close1 > open1;
   bool engulf   = (open1 <= close2) && (close1 >= open2);

   return (bearish2 && bullish1 && engulf);
}

bool IsBearishEngulfing()
{
   double open1 = iOpen(_Symbol,_Period,1), close1 = iClose(_Symbol,_Period,1);
   double open2 = iOpen(_Symbol,_Period,2), close2 = iClose(_Symbol,_Period,2);

   bool bullish2 = close2 > open2;
   bool bearish1 = close1 < open1;
   bool engulf   = (open1 >= close2) && (close1 <= open2);

   return (bullish2 && bearish1 && engulf);
}

//+------------------------------------------------------------------+
//| Svecha kuchini aniqlash -> RR qaytaradi (2 yoki 3)                |
//+------------------------------------------------------------------+
double GetRewardRatio()
{
   double atr[];
   ArraySetAsSeries(atr, true);
   if(CopyBuffer(handleATR, 0, 1, 1, atr) < 1) return 2.0;

   double range1 = iHigh(_Symbol,_Period,1) - iLow(_Symbol,_Period,1);

   if(range1 > atr[0]*InpATRMultiplier)
      return 3.0;
   return 2.0;
}

//+------------------------------------------------------------------+
bool IsLowerTF()
{
   return (_Period == PERIOD_M5 || _Period == PERIOD_M15);
}

bool IsStrongEnoughForCounterTrend()
{
   double atr[];
   ArraySetAsSeries(atr, true);
   if(CopyBuffer(handleATR, 0, 1, 1, atr) < 1) return false;

   double range1 = iHigh(_Symbol,_Period,1) - iLow(_Symbol,_Period,1);
   return (range1 > atr[0]*InpCounterTrendATRMultiplier);
}

//+------------------------------------------------------------------+
//| Risk asosida lot hisoblash                                        |
//+------------------------------------------------------------------+
double CalculateLot(double slDistancePrice, bool isCounterTrend=false, int setupType=-1)
{
   if(!InpUseRiskPercent) return NormalizeLot(InpFixedLot);

   double balance   = AccountInfoDouble(ACCOUNT_BALANCE);
   double riskPct   = isCounterTrend ? InpRiskPercent*InpCounterTrendRiskMultiplier : InpRiskPercent;

   if(setupType>=0)
   {
      double confMult = GetConfidenceMultiplier(setupType);
      riskPct *= confMult;
   }

   if(InpReduceRiskAfterLoss)
   {
      double lastResult = GlobalVariableGet(gvLastResult);
      if(lastResult == 0)      riskPct *= InpPostLossRiskMultiplier;
      else if(lastResult == 1) riskPct *= InpPostWinRiskMultiplier;
   }

   if(riskPct > InpAbsoluteMaxRiskPercent) riskPct = InpAbsoluteMaxRiskPercent;

   double riskMoney = balance * riskPct / 100.0;

   double tickValue = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_VALUE);
   double tickSize  = SymbolInfoDouble(_Symbol, SYMBOL_TRADE_TICK_SIZE);
   if(tickSize<=0 || tickValue<=0) return NormalizeLot(InpFixedLot);

   double lossPerLot = (slDistancePrice / tickSize) * tickValue;
   if(lossPerLot<=0) return NormalizeLot(InpFixedLot);

   double lot = riskMoney / lossPerLot;

   if(lot < InpMinLot) lot = InpMinLot;
   return NormalizeLot(lot);
}

double NormalizeLot(double lot)
{
   double stepLot = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   double minLot  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN);
   double maxLot  = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MAX);

   lot = MathFloor(lot/stepLot)*stepLot;
   if(lot < minLot) lot = minLot;
   if(lot > maxLot) lot = maxLot;
   return NormalizeDouble(lot, 2);
}

//+------------------------------------------------------------------+
int GetSetupTypeFromComment(string comment)
{
   bool isCounter = (StringFind(comment, "CounterTrend") >= 0);
   bool isBuy     = (StringFind(comment, "Buy") >= 0);
   if(!isCounter && isBuy)  return 0;
   if(!isCounter && !isBuy) return 1;
   if(isCounter && isBuy)   return 2;
   return 3;
}

string SetupTypeNameTajik(int idx)
{
   if(idx==0) return "Тибқи тамоюл (Buy)";
   if(idx==1) return "Тибқи тамоюл (Sell)";
   if(idx==2) return "Зидди тамоюл (Buy)";
   if(idx==3) return "Зидди тамоюл (Sell)";
   return "Номаълум";
}

//+------------------------------------------------------------------+
int GetEntryHour(ulong closeDealTicket)
{
   long posId = HistoryDealGetInteger(closeDealTicket, DEAL_POSITION_ID);
   int hour = -1;

   if(HistorySelectByPosition(posId))
   {
      int n = HistoryDealsTotal();
      for(int i=0; i<n; i++)
      {
         ulong t = HistoryDealGetTicket(i);
         if(HistoryDealGetInteger(t, DEAL_ENTRY) == DEAL_ENTRY_IN)
         {
            datetime dt = (datetime)HistoryDealGetInteger(t, DEAL_TIME);
            MqlDateTime tm;
            TimeToStruct(dt, tm);
            hour = tm.hour;
            break;
         }
      }
   }

   HistorySelect(0, TimeCurrent());
   return hour;
}

//+------------------------------------------------------------------+
bool GetEntryPriceAndDirection(ulong closeDealTicket, double &openPrice, bool &isBuy)
{
   long posId = HistoryDealGetInteger(closeDealTicket, DEAL_POSITION_ID);
   bool found = false;

   if(HistorySelectByPosition(posId))
   {
      int n = HistoryDealsTotal();
      for(int i=0; i<n; i++)
      {
         ulong t = HistoryDealGetTicket(i);
         if(HistoryDealGetInteger(t, DEAL_ENTRY) == DEAL_ENTRY_IN)
         {
            openPrice = HistoryDealGetDouble(t, DEAL_PRICE);
            isBuy = (HistoryDealGetInteger(t, DEAL_TYPE) == DEAL_TYPE_BUY);
            found = true;
            break;
         }
      }
   }
   HistorySelect(0, TimeCurrent());
   return found;
}

//+------------------------------------------------------------------+
void UpdateStats(int setupType, int entryHour, bool isWin, double profit)
{
   string keyAllSetupW = "EA_AllSetupW_" + statsKey + "_" + IntegerToString(setupType);
   string keyAllSetupL = "EA_AllSetupL_" + statsKey + "_" + IntegerToString(setupType);
   if(isWin) GlobalVariableSet(keyAllSetupW, GlobalVariableGet(keyAllSetupW) + 1);
   else      GlobalVariableSet(keyAllSetupL, GlobalVariableGet(keyAllSetupL) + 1);

   if(entryHour >= 0 && entryHour <= 23)
   {
      string keyAllHourW = "EA_AllHourW_" + statsKey + "_" + IntegerToString(entryHour);
      string keyAllHourL = "EA_AllHourL_" + statsKey + "_" + IntegerToString(entryHour);
      if(isWin) GlobalVariableSet(keyAllHourW, GlobalVariableGet(keyAllHourW) + 1);
      else      GlobalVariableSet(keyAllHourL, GlobalVariableGet(keyAllHourL) + 1);
   }

   string keyDaySetupW = "EA_DaySetupW_" + statsKey + "_" + IntegerToString(setupType);
   string keyDaySetupL = "EA_DaySetupL_" + statsKey + "_" + IntegerToString(setupType);
   if(!GlobalVariableCheck(keyDaySetupW)) GlobalVariableSet(keyDaySetupW, 0);
   if(!GlobalVariableCheck(keyDaySetupL)) GlobalVariableSet(keyDaySetupL, 0);
   if(isWin) GlobalVariableSet(keyDaySetupW, GlobalVariableGet(keyDaySetupW) + 1);
   else      GlobalVariableSet(keyDaySetupL, GlobalVariableGet(keyDaySetupL) + 1);

   GlobalVariableSet(gvDayTrades, GlobalVariableGet(gvDayTrades) + 1);
   if(isWin) GlobalVariableSet(gvDayWins, GlobalVariableGet(gvDayWins) + 1);
   else      GlobalVariableSet(gvDayLosses, GlobalVariableGet(gvDayLosses) + 1);
   GlobalVariableSet(gvDayProfitSum, GlobalVariableGet(gvDayProfitSum) + profit);
}

//+------------------------------------------------------------------+
void GenerateDailyReportTajik()
{
   int    dayTrades = (int)GlobalVariableGet(gvDayTrades);
   int    dayWins   = (int)GlobalVariableGet(gvDayWins);
   int    dayLosses = (int)GlobalVariableGet(gvDayLosses);
   double dayProfit = GlobalVariableGet(gvDayProfitSum);

   if(dayTrades == 0)
   {
      Print("[Ҳисоботи рӯзона] Имрӯз ягон савдо анҷом наёфт.");
      return;
   }

   double winRate = (dayTrades>0) ? (double)dayWins/dayTrades*100.0 : 0;

   string report = "";
   report += "====== ҲИСОБОТИ РӮЗОНА (EngulfingTrendEA v2.00) ======\n";
   report += "Сана: " + TimeToString(TimeCurrent()-86400, TIME_DATE) + "\n";
   report += "Асбоб: " + _Symbol + " | Даврача: " + EnumToString((ENUM_TIMEFRAMES)_Period) + "\n";
   report += "Шумораи умумии савдоҳо: " + IntegerToString(dayTrades) + "\n";
   report += "Ғалаба: " + IntegerToString(dayWins) + " | Зарар: " + IntegerToString(dayLosses) +
             " | Фоизи ғалаба: " + DoubleToString(winRate,1) + "%\n";
   report += "Натиҷаи молиявии рӯз: " + DoubleToString(dayProfit,2) + " " + AccountInfoString(ACCOUNT_CURRENCY) + "\n";
   report += "\nТаҳлил аз рӯи навъи сигнал (имрӯз):\n";

   int worstIdx = -1; double worstWR = 999;
   for(int s=0; s<4; s++)
   {
      string kw = "EA_DaySetupW_" + statsKey + "_" + IntegerToString(s);
      string kl = "EA_DaySetupL_" + statsKey + "_" + IntegerToString(s);
      int w = GlobalVariableCheck(kw) ? (int)GlobalVariableGet(kw) : 0;
      int l = GlobalVariableCheck(kl) ? (int)GlobalVariableGet(kl) : 0;
      int t = w+l;
      if(t==0) continue;
      double wr = (double)w/t*100.0;
      report += " - " + SetupTypeNameTajik(s) + ": " + IntegerToString(w) + "/" + IntegerToString(t) +
                " ғалаба (" + DoubleToString(wr,1) + "%)\n";
      if(wr < worstWR) { worstWR = wr; worstIdx = s; }
   }

   int bestHour=-1, worstHour=-1; double bestWR=-1, worstHourWR=101;
   for(int h=0; h<24; h++)
   {
      string kw = "EA_AllHourW_" + statsKey + "_" + IntegerToString(h);
      string kl = "EA_AllHourL_" + statsKey + "_" + IntegerToString(h);
      int w = GlobalVariableCheck(kw) ? (int)GlobalVariableGet(kw) : 0;
      int l = GlobalVariableCheck(kl) ? (int)GlobalVariableGet(kl) : 0;
      int t = w+l;
      if(t < 3) continue;
      double wr = (double)w/t*100.0;
      if(wr > bestWR)  { bestWR = wr; bestHour = h; }
      if(wr < worstHourWR) { worstHourWR = wr; worstHour = h; }
   }

   report += "\nКамбудиҳо ва тавсияҳо:\n";
   if(worstIdx >= 0 && worstWR < 45.0)
      report += " - Навъи сигнали \"" + SetupTypeNameTajik(worstIdx) + "\" имрӯз натиҷаи заиф дод (" +
                DoubleToString(worstWR,1) + "%). Тавсия: ин навъро бодиққат назорат кунед.\n";
   else
      report += " - Имрӯз камбудии ҷиддӣ мушоҳида нашуд.\n";

   if(bestHour >= 0)
      report += " - Соати беҳтарин (тамоми давра): соати " + IntegerToString(bestHour) +
                ":00 (Ғалаба " + DoubleToString(bestWR,1) + "%)\n";
   if(worstHour >= 0)
      report += " - Соати заифтарин (тамоми давра): соати " + IntegerToString(worstHour) +
                ":00 (Ғалаба " + DoubleToString(worstHourWR,1) + "%) — эҳтиёт бошед.\n";
   if(bestHour < 0 && worstHour < 0)
      report += " - Ҳанӯз маълумот барои таҳлили соатҳо кофӣ нест.\n";

   string kCount = "EA_MAECount_" + statsKey;
   if(GlobalVariableCheck(kCount) && GlobalVariableGet(kCount) >= 5)
   {
      double avgMFE = GlobalVariableGet("EA_AvgMFE_" + statsKey);
      double avgMAE = GlobalVariableGet("EA_AvgMAE_" + statsKey);
      report += " - Ба таври миёна нарх ба фоида "+DoubleToString(avgMFE,2)+"R меравад, аммо пеш аз он то "+
                DoubleToString(avgMAE,2)+"R бар зидди савдо ҳаракат мекунад.\n";
   }

   report += "=================================================";

   if(InpPrintDailyReportTajik) Print(report);

   if(InpSaveDailyReportToFile)
   {
      int fh = FileOpen(reportFileName, FILE_READ|FILE_WRITE|FILE_TXT|FILE_COMMON);
      if(fh != INVALID_HANDLE)
      {
         FileSeek(fh, 0, SEEK_END);
         FileWriteString(fh, report + "\n\n");
         FileClose(fh);
      }
   }

   GlobalVariableSet(gvDayTrades, 0);
   GlobalVariableSet(gvDayWins, 0);
   GlobalVariableSet(gvDayLosses, 0);
   GlobalVariableSet(gvDayProfitSum, 0);
   for(int s=0; s<4; s++)
   {
      GlobalVariableSet("EA_DaySetupW_" + statsKey + "_" + IntegerToString(s), 0);
      GlobalVariableSet("EA_DaySetupL_" + statsKey + "_" + IntegerToString(s), 0);
   }
}

//+------------------------------------------------------------------+
bool IsHigherTFTrendConfirmed(int currentTrend)
{
   if(!InpUseHigherTFConfirm) return true;
   if(higherTF == PERIOD_CURRENT) return true;
   if(handleHigherFastMA==INVALID_HANDLE || handleHigherSlowMA==INVALID_HANDLE) return true;

   double hFast[], hSlow[];
   ArraySetAsSeries(hFast,true); ArraySetAsSeries(hSlow,true);
   if(CopyBuffer(handleHigherFastMA,0,1,1,hFast)<1) return true;
   if(CopyBuffer(handleHigherSlowMA,0,1,1,hSlow)<1) return true;

   int higherTrend = (hFast[0]>hSlow[0]) ? 1 : (hFast[0]<hSlow[0] ? -1 : 0);
   return (higherTrend == currentTrend);
}

//+------------------------------------------------------------------+
double GetConfidenceMultiplier(int setupType)
{
   if(!InpUseConfidenceScaling) return 1.0;

   string kw = "EA_AllSetupW_" + statsKey + "_" + IntegerToString(setupType);
   string kl = "EA_AllSetupL_" + statsKey + "_" + IntegerToString(setupType);
   int w = GlobalVariableCheck(kw) ? (int)GlobalVariableGet(kw) : 0;
   int l = GlobalVariableCheck(kl) ? (int)GlobalVariableGet(kl) : 0;
   int total = w+l;

   if(total < InpMinSampleForConfidence) return 1.0;

   double winRate = (double)w/total*100.0;
   if(winRate < InpConfidenceWinRateThreshold) return 1.0;

   double range = 100.0 - InpConfidenceWinRateThreshold;
   if(range<=0) return 1.0;
   double t = (winRate - InpConfidenceWinRateThreshold) / range;
   double mult = 1.0 + t*(InpMaxConfidenceMultiplier - 1.0);

   if(mult > InpMaxConfidenceMultiplier) mult = InpMaxConfidenceMultiplier;
   return mult;
}

//+------------------------------------------------------------------+
void AnalyzeMAEMFE(ulong closeDealTicket, double openPrice, double initialRisk, bool isBuyDir)
{
   if(initialRisk<=0) return;

   long posId = HistoryDealGetInteger(closeDealTicket, DEAL_POSITION_ID);
   datetime openTime=0, closeTime=(datetime)HistoryDealGetInteger(closeDealTicket, DEAL_TIME);

   if(HistorySelectByPosition(posId))
   {
      int n = HistoryDealsTotal();
      for(int i=0;i<n;i++)
      {
         ulong t = HistoryDealGetTicket(i);
         if(HistoryDealGetInteger(t, DEAL_ENTRY)==DEAL_ENTRY_IN)
         { openTime = (datetime)HistoryDealGetInteger(t, DEAL_TIME); break; }
      }
   }
   HistorySelect(0, TimeCurrent());
   if(openTime==0) return;

   int barFrom = iBarShift(_Symbol, _Period, openTime, false);
   int barTo   = iBarShift(_Symbol, _Period, closeTime, false);
   if(barFrom<=0 || barTo<0 || barFrom<barTo) return;

   double extremeFavor = isBuyDir ? -DBL_MAX : DBL_MAX;
   double extremeAdverse = isBuyDir ? DBL_MAX : -DBL_MAX;

   for(int b=barTo; b<=barFrom; b++)
   {
      double hi = iHigh(_Symbol, _Period, b);
      double lo = iLow(_Symbol, _Period, b);
      if(isBuyDir)
      {
         if(hi > extremeFavor) extremeFavor = hi;
         if(lo < extremeAdverse) extremeAdverse = lo;
      }
      else
      {
         if(lo < extremeFavor) extremeFavor = lo;
         if(hi > extremeAdverse) extremeAdverse = hi;
      }
   }

   double mfeR = isBuyDir ? (extremeFavor-openPrice)/initialRisk : (openPrice-extremeFavor)/initialRisk;
   double maeR = isBuyDir ? (openPrice-extremeAdverse)/initialRisk : (extremeAdverse-openPrice)/initialRisk;

   string kCount = "EA_MAECount_" + statsKey;
   string kMFE   = "EA_AvgMFE_" + statsKey;
   string kMAE   = "EA_AvgMAE_" + statsKey;
   double cnt = GlobalVariableCheck(kCount) ? GlobalVariableGet(kCount) : 0;
   double avgMFE = GlobalVariableCheck(kMFE) ? GlobalVariableGet(kMFE) : 0;
   double avgMAE = GlobalVariableCheck(kMAE) ? GlobalVariableGet(kMAE) : 0;

   avgMFE = (avgMFE*cnt + mfeR) / (cnt+1);
   avgMAE = (avgMAE*cnt + maeR) / (cnt+1);
   cnt += 1;

   GlobalVariableSet(kCount, cnt);
   GlobalVariableSet(kMFE, avgMFE);
   GlobalVariableSet(kMAE, avgMAE);
}

//+------------------------------------------------------------------+
double SwingHighFrom(int startShift, int lookback)
{
   double hh = -1;
   for(int i=startShift; i<startShift+lookback; i++)
   {
      double h = iHigh(_Symbol, _Period, i);
      if(h > hh) hh = h;
   }
   return hh;
}
double SwingLowFrom(int startShift, int lookback)
{
   double ll = -1;
   for(int i=startShift; i<startShift+lookback; i++)
   {
      double l = iLow(_Symbol, _Period, i);
      if(ll < 0 || l < ll) ll = l;
   }
   return ll;
}

//+------------------------------------------------------------------+
struct BreakoutInfo
{
   int    direction;
   bool   isStrong;
   double bodyRatio;
   double closePos;
   double levelBroken;
};

BreakoutInfo DetectBreakout()
{
   BreakoutInfo bi; bi.direction=0; bi.isStrong=false; bi.bodyRatio=0; bi.closePos=0; bi.levelBroken=0;

   double swingHigh = SwingHighFrom(2, InpSwingLookback);
   double swingLow  = SwingLowFrom(2, InpSwingLookback);

   double o = iOpen(_Symbol,_Period,1), c = iClose(_Symbol,_Period,1);
   double h = iHigh(_Symbol,_Period,1), l = iLow(_Symbol,_Period,1);
   double range = h-l;
   if(range<=0) return bi;

   double body = MathAbs(c-o);
   bi.bodyRatio = body/range;

   if(c > swingHigh)
   {
      bi.direction = 1;
      bi.levelBroken = swingHigh;
      bi.closePos = (c-l)/range;
   }
   else if(c < swingLow)
   {
      bi.direction = -1;
      bi.levelBroken = swingLow;
      bi.closePos = (h-c)/range;
   }
   else return bi;

   bi.isStrong = (bi.bodyRatio >= InpStrongBodyRatio && bi.closePos >= InpStrongClosePos);
   return bi;
}

//+------------------------------------------------------------------+
bool IsWPatternPresent()
{
   double atr[]; ArraySetAsSeries(atr,true);
   if(CopyBuffer(handleATR,0,1,1,atr)<1) return false;
   double tol = atr[0]*InpSwingTolerancePctATR;

   double low1 = SwingLowFrom(2, InpSwingLookback/2);
   double low2 = SwingLowFrom(2+InpSwingLookback/2, InpSwingLookback/2);

   return (MathAbs(low1-low2) <= tol);
}

bool IsMPatternPresent()
{
   double atr[]; ArraySetAsSeries(atr,true);
   if(CopyBuffer(handleATR,0,1,1,atr)<1) return false;
   double tol = atr[0]*InpSwingTolerancePctATR;

   double high1 = SwingHighFrom(2, InpSwingLookback/2);
   double high2 = SwingHighFrom(2+InpSwingLookback/2, InpSwingLookback/2);

   return (MathAbs(high1-high2) <= tol);
}

//+------------------------------------------------------------------+
void RunHistoricalBreakoutScan()
{
   Print("Tarixiy tahlil boshlandi (bir martalik jarayon)...");

   int available = Bars(_Symbol, _Period);
   int n = MathMin(available - InpHistForwardBars - InpSwingLookback - 5, InpHistScanBars);
   if(n <= 0) { Print("Tarixiy ma'lumot yetarli emas."); GlobalVariableSet(gvHistDone, 1); return; }

   long strongWin=0, strongTotal=0, weakWin=0, weakTotal=0;

   for(int shift = n; shift >= InpHistForwardBars+1; shift--)
   {
      double swingHigh = SwingHighFrom(shift+1, InpSwingLookback);
      double swingLow  = SwingLowFrom(shift+1, InpSwingLookback);

      double o=iOpen(_Symbol,_Period,shift), c=iClose(_Symbol,_Period,shift);
      double h=iHigh(_Symbol,_Period,shift), l=iLow(_Symbol,_Period,shift);
      double range=h-l;
      if(range<=0) continue;

      double body=MathAbs(c-o);
      double bodyRatio=body/range;

      double sumRange=0;
      for(int k=shift+1;k<shift+1+14;k++) sumRange += (iHigh(_Symbol,_Period,k)-iLow(_Symbol,_Period,k));
      double approxATR = sumRange/14.0;
      if(approxATR<=0) continue;

      int dir=0; double closePos=0;
      if(c > swingHigh) { dir=1; closePos=(c-l)/range; }
      else if(c < swingLow) { dir=-1; closePos=(h-c)/range; }
      else continue;

      bool strong = (bodyRatio>=InpStrongBodyRatio && closePos>=InpStrongClosePos);

      bool continued=false;
      for(int f=shift-1; f>=shift-InpHistForwardBars; f--)
      {
         if(f<0) break;
         double fc = iClose(_Symbol,_Period,f);
         if(dir==1 && fc >= c + approxATR*InpHistContinueATR) { continued=true; break; }
         if(dir==-1 && fc <= c - approxATR*InpHistContinueATR) { continued=true; break; }
      }

      if(strong) { strongTotal++; if(continued) strongWin++; }
      else       { weakTotal++;   if(continued) weakWin++; }
   }

   GlobalVariableSet(gvHistStrongWin, (double)strongWin);
   GlobalVariableSet(gvHistStrongTotal, (double)strongTotal);
   GlobalVariableSet(gvHistWeakWin, (double)weakWin);
   GlobalVariableSet(gvHistWeakTotal, (double)weakTotal);
   GlobalVariableSet(gvHistDone, 1);

   double strongWR = strongTotal>0 ? (double)strongWin/strongTotal*100.0 : 0;
   double weakWR   = weakTotal>0   ? (double)weakWin/weakTotal*100.0     : 0;

   Print("Tarixiy tahlil tugadi. Kuchli: ", strongWin,"/",strongTotal," (",DoubleToString(strongWR,1),
         "%) | Zaif: ", weakWin,"/",weakTotal," (",DoubleToString(weakWR,1),"%)");
}

//+------------------------------------------------------------------+
void GetHistoricalWinRate(bool strong, double &winRate, long &sampleSize)
{
   if(strong)
   {
      sampleSize = (long)GlobalVariableGet(gvHistStrongTotal);
      long win    = (long)GlobalVariableGet(gvHistStrongWin);
      winRate = sampleSize>0 ? (double)win/sampleSize*100.0 : 0;
   }
   else
   {
      sampleSize = (long)GlobalVariableGet(gvHistWeakTotal);
      long win    = (long)GlobalVariableGet(gvHistWeakWin);
      winRate = sampleSize>0 ? (double)win/sampleSize*100.0 : 0;
   }
}

//+------------------------------------------------------------------+
void AnalyzeAndLogBreakout()
{
   BreakoutInfo bi = DetectBreakout();
   if(bi.direction == 0) return;

   bool wPattern = (bi.direction==1) && IsWPatternPresent();
   bool mPattern = (bi.direction==-1) && IsMPatternPresent();

   double winRate; long sample;
   GetHistoricalWinRate(bi.isStrong, winRate, sample);

   MqlDateTime tm; TimeToStruct(iTime(_Symbol,_Period,1), tm);

   string dirStr = bi.direction==1 ? "YUQORIGA" : "PASTGA";
   string msg = StringFormat(
      "[TAHLIL] %s buzilish | Soat:%02d:00 | Daraja:%.5f | Body/Range:%.2f | ClosePos:%.2f | Kuch:%s | W:%s | M:%s | Tarixiy:%.1f%% (n=%d)",
      dirStr, tm.hour, bi.levelBroken, bi.bodyRatio, bi.closePos,
      (bi.isStrong?"KUCHLI":"ZAIF"), (wPattern?"HA":"YO'Q"), (mPattern?"HA":"YO'Q"), winRate, (int)sample);

   Print(msg);

   int fh = FileOpen("EngulfingEA_BreakoutAnalysis_" + statsKey + ".csv",
                     FILE_READ|FILE_WRITE|FILE_CSV|FILE_COMMON, ',');
   if(fh != INVALID_HANDLE)
   {
      FileSeek(fh, 0, SEEK_END);
      FileWrite(fh, TimeToString(iTime(_Symbol,_Period,1),TIME_DATE|TIME_MINUTES), dirStr,
                DoubleToString(bi.levelBroken,5), DoubleToString(bi.bodyRatio,2), DoubleToString(bi.closePos,2),
                (bi.isStrong?"KUCHLI":"ZAIF"), (wPattern?"W_HA":"W_YOQ"), (mPattern?"M_HA":"M_YOQ"),
                DoubleToString(winRate,1)+"%", (int)sample);
      FileClose(fh);
   }
}

//+------------------------------------------------------------------+
bool IsPaused()
{
   double pauseUntil = GlobalVariableGet(gvPauseUntil);
   if(pauseUntil > (double)TimeCurrent())
      return true;
   return false;
}

//+------------------------------------------------------------------+
void UpdateTradeMemory()
{
   if(!HistorySelect(0, TimeCurrent())) return;

   int total = HistoryDealsTotal();
   if(total==0) return;

   ulong lastTicket = (ulong)GlobalVariableGet(gvLastTicket);
   ulong newestTicket = HistoryDealGetTicket(total-1);

   if(newestTicket == lastTicket) return;

   for(int i=total-1; i>=0; i--)
   {
      ulong ticket = HistoryDealGetTicket(i);
      if(ticket <= lastTicket) break;

      if(HistoryDealGetInteger(ticket, DEAL_MAGIC) != InpMagicNumber) continue;
      if(HistoryDealGetString(ticket, DEAL_SYMBOL) != _Symbol) continue;
      if(HistoryDealGetInteger(ticket, DEAL_ENTRY) != DEAL_ENTRY_OUT) continue;

      double profit = HistoryDealGetDouble(ticket, DEAL_PROFIT);
      datetime t    = (datetime)HistoryDealGetInteger(ticket, DEAL_TIME);
      string dealComment = HistoryDealGetString(ticket, DEAL_COMMENT);

      int setupType = GetSetupTypeFromComment(dealComment);
      int entryHour = GetEntryHour(ticket);
      UpdateStats(setupType, entryHour, (profit>=0), profit);
      GlobalVariableSet(gvLastResult, (profit>=0) ? 1 : 0);

      long posIdClosed = HistoryDealGetInteger(ticket, DEAL_POSITION_ID);
      string gvInitRiskClosed = "EA_InitRisk_" + IntegerToString(posIdClosed);
      double initRiskForAnalysis = GlobalVariableCheck(gvInitRiskClosed) ? GlobalVariableGet(gvInitRiskClosed) : 0;
      double entryOpenPrice; bool entryIsBuy;
      if(initRiskForAnalysis>0 && GetEntryPriceAndDirection(ticket, entryOpenPrice, entryIsBuy))
         AnalyzeMAEMFE(ticket, entryOpenPrice, initRiskForAnalysis, entryIsBuy);

      GlobalVariableDel(gvInitRiskClosed);
      GlobalVariableDel("EA_Ladder_" + IntegerToString(posIdClosed));

      int consecLosses = (int)GlobalVariableGet(gvConsecLosses);
      if(profit < 0)
      {
         consecLosses++;
         if(consecLosses >= InpMaxConsecLosses)
         {
            datetime pauseUntil = TimeCurrent() + InpPauseHours*3600;
            GlobalVariableSet(gvPauseUntil, (double)pauseUntil);
            Print("Diqqat: ", InpMaxConsecLosses, " ketma-ket zarar. EA ", InpPauseHours, " soatga pauza.");
         }
      }
      else
      {
         consecLosses = 0;
      }
      GlobalVariableSet(gvConsecLosses, consecLosses);

      int fh = FileOpen(logFileName, FILE_READ|FILE_WRITE|FILE_CSV|FILE_COMMON, ',');
      if(fh != INVALID_HANDLE)
      {
         FileSeek(fh, 0, SEEK_END);
         FileWrite(fh, TimeToString(t, TIME_DATE|TIME_MINUTES), ticket,
                   (profit>=0 ? "PROFIT" : "LOSS"), DoubleToString(profit,2),
                   "ConsecLosses="+IntegerToString(consecLosses));
         FileClose(fh);
      }
   }

   GlobalVariableSet(gvLastTicket, (double)newestTicket);
}

//+------------------------------------------------------------------+
bool CheckDailyLossLimit()
{
   if(!InpUseDailyLossLimit) return false;

   MqlDateTime tmNow;
   TimeToStruct(TimeCurrent(), tmNow);
   double todayKey = tmNow.year*10000 + tmNow.mon*100 + tmNow.day;

   double storedDate = GlobalVariableGet(gvDayStartDate);
   if(storedDate != todayKey)
   {
      if(storedDate != 0)
         GenerateDailyReportTajik();

      GlobalVariableSet(gvDayStartDate, todayKey);
      GlobalVariableSet(gvDayStartBalance, AccountInfoDouble(ACCOUNT_BALANCE));
      GlobalVariableSet(gvDailyPaused, 0);
      return false;
   }

   if(GlobalVariableGet(gvDailyPaused) >= 1) return true;

   double dayStart = GlobalVariableGet(gvDayStartBalance);
   double equity   = AccountInfoDouble(ACCOUNT_EQUITY);
   double lossPct  = (dayStart - equity) / dayStart * 100.0;

   if(lossPct >= InpMaxDailyLossPct)
   {
      GlobalVariableSet(gvDailyPaused, 1);
      Print("DIQQAT: Kunlik zarar limiti (", InpMaxDailyLossPct, "%) ga yetdi.");
      return true;
   }
   return false;
}

//+------------------------------------------------------------------+
bool IsSessionAllowed()
{
   if(!InpUseSessionFilter) return true;

   MqlDateTime tm;
   TimeToStruct(TimeCurrent(), tm);
   int h = tm.hour;

   if(InpSessionStartHour <= InpSessionEndHour)
      return (h >= InpSessionStartHour && h <= InpSessionEndHour);
   else
      return (h >= InpSessionStartHour || h <= InpSessionEndHour);
}

//+------------------------------------------------------------------+
bool SafeSendOrder(bool isBuy, double lot, double entry, double sl, double tp, string comment)
{
   double marginNeeded=0;
   ENUM_ORDER_TYPE otype = isBuy ? ORDER_TYPE_BUY : ORDER_TYPE_SELL;
   if(OrderCalcMargin(otype, _Symbol, lot, entry, marginNeeded))
   {
      double freeMargin = AccountInfoDouble(ACCOUNT_MARGIN_FREE);
      if(marginNeeded > freeMargin)
      {
         Print("Xato: Marja yetarli emas. Kerak=", marginNeeded, " Mavjud=", freeMargin);
         return false;
      }
   }

   for(int attempt=1; attempt<=InpMaxRetries; attempt++)
   {
      bool result = isBuy ? trade.Buy(lot, _Symbol, entry, sl, tp, comment)
                           : trade.Sell(lot, _Symbol, entry, sl, tp, comment);
      if(result) return true;

      int err = GetLastError();
      Print("Order xatoligi (urinish ", attempt, "/", InpMaxRetries, "): ", err,
            " - ", trade.ResultRetcodeDescription());
      ResetLastError();

      if(isBuy) entry = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
      else      entry = SymbolInfoDouble(_Symbol, SYMBOL_BID);

      Sleep(InpRetryDelayMs);
   }
   Print("Order ", InpMaxRetries, " marta urinishdan keyin ham muvaffaqiyatsiz.");
   return false;
}

//+------------------------------------------------------------------+
bool IsStrongTrendNow()
{
   double fastMA[], slowMA[], atr[];
   ArraySetAsSeries(fastMA,true); ArraySetAsSeries(slowMA,true); ArraySetAsSeries(atr,true);
   if(CopyBuffer(handleFastMA,0,1,1,fastMA)<1) return false;
   if(CopyBuffer(handleSlowMA,0,1,1,slowMA)<1) return false;
   if(CopyBuffer(handleATR,0,1,1,atr)<1) return false;
   if(atr[0]<=0) return false;

   double ratio = MathAbs(fastMA[0]-slowMA[0]) / atr[0];
   return (ratio >= InpStrongTrendATRRatio);
}

//+------------------------------------------------------------------+
void ApplyDynamicTrailing()
{
   if(!InpUseBreakEven) return;

   for(int i=0; i<PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(!PositionSelectByTicket(ticket)) continue;
      if(PositionGetString(POSITION_SYMBOL)!=_Symbol) continue;
      if(PositionGetInteger(POSITION_MAGIC)!=InpMagicNumber) continue;

      double openPrice = PositionGetDouble(POSITION_PRICE_OPEN);
      double curSL      = PositionGetDouble(POSITION_SL);
      double curTP      = PositionGetDouble(POSITION_TP);
      long   type       = PositionGetInteger(POSITION_TYPE);

      string gvInitRisk = "EA_InitRisk_" + IntegerToString(ticket);
      string gvLadder   = "EA_Ladder_" + IntegerToString(ticket);
      double initialRisk = GlobalVariableCheck(gvInitRisk) ? GlobalVariableGet(gvInitRisk) : MathAbs(openPrice-curSL);
      bool   useLadder   = GlobalVariableCheck(gvLadder) && GlobalVariableGet(gvLadder) >= 1;
      if(initialRisk<=0) continue;

      double buffer = InpBreakEvenBufferPoints*_Point;

      if(type==POSITION_TYPE_BUY)
      {
         double curPrice = SymbolInfoDouble(_Symbol, SYMBOL_BID);
         double profitR  = (curPrice - openPrice) / initialRisk;
         double lockR = -1;

         if(useLadder && profitR >= InpLadderTrigger3R) lockR = 3.0;
         else if(useLadder && profitR >= InpLadderTrigger2R) lockR = 2.0;
         else if(profitR >= InpBreakEvenRR) lockR = 0.0;

         if(lockR > -1)
         {
            double newSL = openPrice + lockR*initialRisk + buffer;
            if(curSL < newSL)
               trade.PositionModify(ticket, newSL, curTP);
         }
      }
      else if(type==POSITION_TYPE_SELL)
      {
         double curPrice = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
         double profitR  = (openPrice - curPrice) / initialRisk;
         double lockR = -1;

         if(useLadder && profitR >= InpLadderTrigger3R) lockR = 3.0;
         else if(useLadder && profitR >= InpLadderTrigger2R) lockR = 2.0;
         else if(profitR >= InpBreakEvenRR) lockR = 0.0;

         if(lockR > -1)
         {
            double newSL = openPrice - lockR*initialRisk - buffer;
            if(curSL > newSL || curSL==0)
               trade.PositionModify(ticket, newSL, curTP);
         }
      }
   }
}

//+------------------------------------------------------------------+
//| Dashboard (TO'LIQ ISLOH QILINGAN)                                 |
//+------------------------------------------------------------------+
void ShowDashboard(int trend, bool dailyPaused, bool consecPaused)
{
   double dayStart = GlobalVariableGet(gvDayStartBalance);
   double equity   = AccountInfoDouble(ACCOUNT_EQUITY);
   double dailyPL  = equity - dayStart;
   double dailyPLPct = dayStart>0 ? dailyPL/dayStart*100.0 : 0;
   string trendStr = trend==1 ? "UP" : (trend==-1 ? "DOWN" : "RANGE");
   int consecLosses = (int)GlobalVariableGet(gvConsecLosses);
   double spread = (double)SymbolInfoInteger(_Symbol, SYMBOL_SPREAD);

   // YANGI: Flat bozor holatini aniqlash uchun
   string flatStatus = "OK";
   if(InpUseADXFilter)
   {
      double adx[];
      ArraySetAsSeries(adx, true);
      if(CopyBuffer(handleADX, 0, 1, 1, adx) > 0)
      {
         if(adx[0] < InpADXMin) flatStatus = "FLAT (ADX="+DoubleToString(adx[0],1)+")";
         else                   flatStatus = "TREND (ADX="+DoubleToString(adx[0],1)+")";
      }
   }

   // Xabar vaqti
   string newsStatus = "OK";
   if(InpUseNewsFilter && IsNewsTime()) newsStatus = "XABAR!";
   if(InpUseRolloverFilter && IsRolloverTime()) newsStatus = "ROLLOVER!";

   string txt = "== EngulfingTrendEA v2.00 ==\n";
   txt += _Symbol + " | TF: " + EnumToString((ENUM_TIMEFRAMES)_Period) + "\n";
   txt += "Trend: " + trendStr + " | " + flatStatus + "\n";
   txt += "Spread: " + DoubleToString(spread,0) + " / max " + DoubleToString(InpMaxSpreadPoints,0) + "\n";
   txt += "Kunlik P/L: " + DoubleToString(dailyPL,2) + " (" + DoubleToString(dailyPLPct,2) + "%)\n";
   txt += "Ketma-ket zarar: " + IntegerToString(consecLosses) + " / " + IntegerToString(InpMaxConsecLosses) + "\n";
   txt += "Vaqt filtri: " + newsStatus + "\n";
   txt += "Holat: " + (dailyPaused ? "KUNLIK PAUZA" : (consecPaused ? "PAUZA" : "FAOL")) + "\n";
   txt += "Filterlar: ADX=" + (InpUseADXFilter?"ON":"OFF") +
          " ATR-Avg=" + (InpUseATRAverageFilter?"ON":"OFF") +
          " EMA-Sep=" + (InpUseEMASeparation?"ON":"OFF") + "\n";
   txt += "Vaqt filter: News=" + (InpUseNewsFilter?"ON":"OFF") +
          " Rollover=" + (InpUseRolloverFilter?"ON":"OFF");

   Comment(txt);
}

//+------------------------------------------------------------------+
bool HasOpenPosition()
{
   for(int i=0; i<PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(PositionSelectByTicket(ticket))
      {
         if(PositionGetString(POSITION_SYMBOL)==_Symbol &&
            PositionGetInteger(POSITION_MAGIC)==InpMagicNumber)
            return true;
      }
   }
   return false;
}

//+------------------------------------------------------------------+
void OnTick()
{
   UpdateTradeMemory();
   ApplyDynamicTrailing();

   bool dailyPaused  = CheckDailyLossLimit();
   bool consecPaused = IsPaused();
   int  trendForBoard = DetectTrend();
   ShowDashboard(trendForBoard, dailyPaused, consecPaused);

   static datetime lastBarTime = 0;
   datetime currentBarTime = iTime(_Symbol, _Period, 0);
   if(currentBarTime == lastBarTime) return;
   lastBarTime = currentBarTime;

   AnalyzeAndLogBreakout();

   if(dailyPaused)
   {
      Print("EA bugun uchun to'xtatilgan (kunlik zarar limiti).");
      return;
   }

   if(consecPaused)
   {
      Print("EA hozircha pauzada (ketma-ket zararlar).");
      return;
   }

   if(!IsSessionAllowed()) return;

   // ===== YANGI: VAQT FILTRLARI =====
   if(IsNewsTime()) return;
   if(IsRolloverTime()) return;

   double spread = (double)SymbolInfoInteger(_Symbol, SYMBOL_SPREAD);
   if(spread > InpMaxSpreadPoints) return;

   if(InpOnlyOnePosition && HasOpenPosition()) return;

   // ===== YANGI: FLAT BOZOR FILTRI =====
   if(IsFlatMarket()) return;

   int trend = trendForBoard;
   if(trend == 0) return;
   if(!IsHigherTFTrendConfirmed(trend)) return;

   double point = _Point;
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   double rr  = GetRewardRatio();

   bool lowerTF = IsLowerTF();

   // ===== BUY =====
   if(trend == 1 && IsBullishEngulfing())
   {
      OpenBuy(ask, rr, false);
      return;
   }

   // ===== SELL =====
   if(trend == -1 && IsBearishEngulfing())
   {
      OpenSell(bid, rr, false);
      return;
   }

   // ===== COUNTER-TREND =====
   if(lowerTF && InpAllowCounterTrendLTF)
   {
      if(trend == 1 && IsBearishEngulfing() && IsStrongEnoughForCounterTrend())
      {
         OpenSell(bid, rr, true);
         return;
      }
      if(trend == -1 && IsBullishEngulfing() && IsStrongEnoughForCounterTrend())
      {
         OpenBuy(ask, rr, true);
         return;
      }
   }
}

//+------------------------------------------------------------------+
void OpenBuy(double entry, double rr, bool isCounterTrend)
{
   double sl = iLow(_Symbol,_Period,2) - InpSLBufferPoints*_Point;
   double slDist = entry - sl;
   if(slDist <= 0) return;

   bool strongTrend = IsStrongTrendNow();
   double effectiveRR = strongTrend ? InpLadderMaxR : rr;
   double tp = entry + slDist*effectiveRR;
   int setupType = isCounterTrend ? 2 : 0;
   double lot = CalculateLot(slDist, isCounterTrend, setupType);
   string tag = (isCounterTrend ? "CounterTrend" : "Trend") + " Buy RR1:"+DoubleToString(rr,0);

   if(SafeSendOrder(true, lot, entry, sl, tp, tag))
   {
      Print(isCounterTrend?"[COUNTER-TREND] ":"[TREND] ","BUY ochildi. Lot=",lot," SL=",sl," TP=",tp,
            " RR=1:",effectiveRR," StrongTrend=",strongTrend);
      SaveNewPositionRisk(slDist, strongTrend);
   }
}

//+------------------------------------------------------------------+
void OpenSell(double entry, double rr, bool isCounterTrend)
{
   double sl = iHigh(_Symbol,_Period,2) + InpSLBufferPoints*_Point;
   double slDist = sl - entry;
   if(slDist <= 0) return;

   bool strongTrend = IsStrongTrendNow();
   double effectiveRR = strongTrend ? InpLadderMaxR : rr;
   double tp = entry - slDist*effectiveRR;
   int setupType = isCounterTrend ? 3 : 1;
   double lot = CalculateLot(slDist, isCounterTrend, setupType);
   string tag = (isCounterTrend ? "CounterTrend" : "Trend") + " Sell RR1:"+DoubleToString(rr,0);

   if(SafeSendOrder(false, lot, entry, sl, tp, tag))
   {
      Print(isCounterTrend?"[COUNTER-TREND] ":"[TREND] ","SELL ochildi. Lot=",lot," SL=",sl," TP=",tp,
            " RR=1:",effectiveRR," StrongTrend=",strongTrend);
      SaveNewPositionRisk(slDist, strongTrend);
   }
}

//+------------------------------------------------------------------+
void SaveNewPositionRisk(double slDist, bool strongTrend)
{
   for(int i=0; i<PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(!PositionSelectByTicket(ticket)) continue;
      if(PositionGetString(POSITION_SYMBOL)!=_Symbol) continue;
      if(PositionGetInteger(POSITION_MAGIC)!=InpMagicNumber) continue;

      GlobalVariableSet("EA_InitRisk_" + IntegerToString(ticket), slDist);
      GlobalVariableSet("EA_Ladder_" + IntegerToString(ticket), strongTrend ? 1 : 0);
   }
}
//+------------------------------------------------------------------+
