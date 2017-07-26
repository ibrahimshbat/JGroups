package org.jgroups.protocols.jzookeeper.zabCT_AdaptationUsingWriteRatioSyncV2;

public class ConstantPara {
	private final static int THETA_N3_WR10_T0     = 1810;
	private final static int THETA_N3_WR20_T0     = 2502;
	private final static int THETA_N3_WR30_T0     = 2970;
	private final static int THETA_N3_WR40_T0     = 3395;
	private final static int THETA_N3_WR50_T0     = 3720;
	private final static int THETA_N3_WR60_T0     = 3810;
	private final static int THETA_N3_WR70_T0     = 3988;
	private final static int THETA_N3_WR80_T0     = 3990;
	private final static int THETA_N3_WR90_T0     = 4037;
	private final static int THETA_N3_WR100_T0    = 4067;
	private final static int THETA_N5_WR10_T0     = 1703;
	private final static int THETA_N5_WR20_T0     = 2070;
	private final static int THETA_N5_WR30_T0     = 2250;
	private final static int THETA_N5_WR40_T0     = 2363;
	private final static int THETA_N5_WR50_T0     = 2411;
	private final static int THETA_N5_WR60_T0     = 2424;
	private final static int THETA_N5_WR70_T0     = 2430;
	private final static int THETA_N5_WR80_T0     = 2435;
	private final static int THETA_N5_WR90_T0     = 2444;
	private final static int THETA_N5_WR100_T0    = 2460;
	private final static int THETA_N7_WR10_T0     = 1410;
	private final static int THETA_N7_WR20_T0     = 1573;
	private final static int THETA_N7_WR30_T0     = 1640;
	private final static int THETA_N7_WR40_T0     = 1675;
	private final static int THETA_N7_WR50_T0     = 1698;
	private final static int THETA_N7_WR60_T0     = 1703;
	private final static int THETA_N7_WR70_T0     = 1713;
	private final static int THETA_N7_WR80_T0     = 1718;
	private final static int THETA_N7_WR90_T0     = 1721;
	private final static int THETA_N7_WR100_T0    = 1716;
	private final static int THETA_N9_WR10_T0     = 1123;
	private final static int THETA_N9_WR20_T0     = 1219;
	private final static int THETA_N9_WR30_T0     = 1250;
	private final static int THETA_N9_WR40_T0     = 1264;
	private final static int THETA_N9_WR50_T0     = 1270;
	private final static int THETA_N9_WR60_T0     = 1279;
	private final static int THETA_N9_WR70_T0     = 1285;
	private final static int THETA_N9_WR80_T0     = 1292;
	private final static int THETA_N9_WR90_T0     = 1298;
	private final static int  THETA_N9_WR100_T0   = 1313;

	private final static int THETA_N3_WR10_T50    = 450;
	private final static int THETA_N3_WR20_T50    = 941;
	private final static int THETA_N3_WR30_T50    = 1398;
	private final static int THETA_N3_WR40_T50    = 1804;
	private final static int THETA_N3_WR50_T50    = 2202;
	private final static int THETA_N3_WR60_T50    = 2570;
	private final static int THETA_N3_WR70_T50    = 2920;
	private final static int THETA_N3_WR80_T50    = 3133;
	private final static int THETA_N3_WR90_T50    = 3269;
	private final static int THETA_N3_WR100_T50   = 3597;
	private final static int THETA_N5_WR10_T50    = 460;
	private final static int THETA_N5_WR20_T50    = 943;
	private final static int THETA_N5_WR30_T50    = 1396;
	private final static int THETA_N5_WR40_T50    = 1833;
	private final static int THETA_N5_WR50_T50    = 2039;
	private final static int THETA_N5_WR60_T50    = 2201;
	private final static int THETA_N5_WR70_T50    = 2211;
	private final static int THETA_N5_WR80_T50    = 2218;
	private final static int THETA_N5_WR90_T50    = 2223;
	private final static int THETA_N5_WR100_T50   = 2236;
	private final static int THETA_N7_WR10_T50    = 455;
	private final static int THETA_N7_WR20_T50    = 984;
	private final static int THETA_N7_WR30_T50    = 1373;
	private final static int THETA_N7_WR40_T50    = 1599;
	private final static int THETA_N7_WR50_T50    = 1650;
	private final static int THETA_N7_WR60_T50    = 1691;
	private final static int THETA_N7_WR70_T50    = 1702;
	private final static int THETA_N7_WR80_T50    = 1710;
	private final static int THETA_N7_WR90_T50    = 1715;
	private final static int THETA_N7_WR100_T50   = 1720;
	private final static int THETA_N9_WR10_T50    = 462;
	private final static int THETA_N9_WR20_T50    = 919;
	private final static int THETA_N9_WR30_T50    = 1210;
	private final static int THETA_N9_WR40_T50    = 1264;
	private final static int THETA_N9_WR50_T50    = 1272;
	private final static int THETA_N9_WR60_T50    = 1278;
	private final static int THETA_N9_WR70_T50    = 1281;
	private final static int THETA_N9_WR80_T50    = 1285;
	private final static int THETA_N9_WR90_T50    = 1290;
	private final static int  THETA_N9_WR100_T50  = 1299;

	public static int findTheta(int waitTime, int N, int wr){
		int theta = 0;
		switch(waitTime){
		case 0:
			switch(N){
			case 3:
				switch(wr){
				case 10:
					theta = THETA_N3_WR10_T0;
					break;
				case 20:
					theta = THETA_N3_WR20_T0;
					break;
				case 30:
					theta = THETA_N3_WR30_T0;
					break;
				case 40:
					theta = THETA_N3_WR40_T0;
					break;
				case 50:
					theta = THETA_N3_WR50_T0;
					break;
				case 60:
					theta = THETA_N3_WR60_T0;
					break;
				case 70:
					theta = THETA_N3_WR70_T0;
					break;
				case 80:
					theta = THETA_N3_WR80_T0;
					break;
				case 90:
					theta = THETA_N3_WR90_T0;
					break;
				case 100:
					theta = THETA_N3_WR100_T0;
					break;					
				}
				break;
			case 5:
				switch(wr){
				case 10:
					theta = THETA_N5_WR10_T0;
					break;
				case 20:
					theta = THETA_N5_WR20_T0;
					break;
				case 30:
					theta = THETA_N5_WR30_T0;
					break;
				case 40:
					theta = THETA_N5_WR40_T0;
					break;
				case 50:
					theta = THETA_N5_WR50_T0;
					break;
				case 60:
					theta = THETA_N5_WR60_T0;
					break;
				case 70:
					theta = THETA_N5_WR70_T0;
					break;
				case 80:
					theta = THETA_N5_WR80_T0;
					break;
				case 90:
					theta = THETA_N5_WR90_T0;
					break;
				case 100:
					theta = THETA_N5_WR100_T0;
					break;					
				}
				break;
			case 7:
				switch(wr){
				case 10:
					theta = THETA_N7_WR10_T0;
					break;
				case 20:
					theta = THETA_N7_WR20_T0;
					break;
				case 30:
					theta = THETA_N7_WR30_T0;
					break;
				case 40:
					theta = THETA_N7_WR40_T0;
					break;
				case 50:
					theta = THETA_N7_WR50_T0;
					break;
				case 60:
					theta = THETA_N7_WR60_T0;
					break;
				case 70:
					theta = THETA_N7_WR70_T0;
					break;
				case 80:
					theta = THETA_N7_WR80_T0;
					break;
				case 90:
					theta = THETA_N7_WR90_T0;
					break;
				case 100:
					theta = THETA_N7_WR100_T0;
					break;					
				}
				break;
			case 9:
				switch(wr){
				case 10:
					theta = THETA_N9_WR10_T0;
					break;
				case 20:
					theta = THETA_N9_WR20_T0;
					break;
				case 30:
					theta = THETA_N9_WR30_T0;
					break;
				case 40:
					theta = THETA_N9_WR40_T0;
					break;
				case 50:
					theta = THETA_N9_WR50_T0;
					break;
				case 60:
					theta = THETA_N9_WR60_T0;
					break;
				case 70:
					theta = THETA_N9_WR70_T0;
					break;
				case 80:
					theta = THETA_N9_WR80_T0;
					break;
				case 90:
					theta = THETA_N9_WR90_T0;
					break;
				case 100:
					theta = THETA_N9_WR100_T0;
					break;					
				}
				break;
			}
			break;
		case 50:
			switch(N){
			case 3:
				switch(wr){
				case 10:
					theta = THETA_N3_WR10_T50;
					break;
				case 20:
					theta = THETA_N3_WR20_T50;
					break;
				case 30:
					theta = THETA_N3_WR30_T50;
					break;
				case 40:
					theta = THETA_N3_WR40_T50;
					break;
				case 50:
					theta = THETA_N3_WR50_T50;
					break;
				case 60:
					theta = THETA_N3_WR60_T50;
					break;
				case 70:
					theta = THETA_N3_WR70_T50;
					break;
				case 80:
					theta = THETA_N3_WR80_T50;
					break;
				case 90:
					theta = THETA_N3_WR90_T50;
					break;
				case 100:
					theta = THETA_N3_WR100_T50;
					break;					
				}
				break;
			case 5:
				switch(wr){
				case 10:
					theta = THETA_N5_WR10_T50;
					break;
				case 20:
					theta = THETA_N5_WR20_T50;
					break;
				case 30:
					theta = THETA_N5_WR30_T50;
					break;
				case 40:
					theta = THETA_N5_WR40_T50;
					break;
				case 50:
					theta = THETA_N5_WR50_T50;
					break;
				case 60:
					theta = THETA_N5_WR60_T50;
					break;
				case 70:
					theta = THETA_N5_WR70_T50;
					break;
				case 80:
					theta = THETA_N5_WR80_T50;
					break;
				case 90:
					theta = THETA_N5_WR90_T50;
					break;
				case 100:
					theta = THETA_N5_WR100_T50;
					break;					
				}
				break;
			case 7:
				switch(wr){
				case 10:
					theta = THETA_N7_WR10_T50;
					break;
				case 20:
					theta = THETA_N7_WR20_T50;
					break;
				case 30:
					theta = THETA_N7_WR30_T50;
					break;
				case 40:
					theta = THETA_N7_WR40_T50;
					break;
				case 50:
					theta = THETA_N7_WR50_T50;
					break;
				case 60:
					theta = THETA_N7_WR60_T50;
					break;
				case 70:
					theta = THETA_N7_WR70_T50;
					break;
				case 80:
					theta = THETA_N7_WR80_T50;
					break;
				case 90:
					theta = THETA_N7_WR90_T50;
					break;
				case 100:
					theta = THETA_N7_WR100_T50;
					break;					
				}
				break;
			case 9:
				switch(wr){
				case 10:
					theta = THETA_N9_WR10_T50;
					break;
				case 20:
					theta = THETA_N9_WR20_T50;
					break;
				case 30:
					theta = THETA_N9_WR30_T50;
					break;
				case 40:
					theta = THETA_N9_WR40_T50;
					break;
				case 50:
					theta = THETA_N9_WR50_T50;
					break;
				case 60:
					theta = THETA_N9_WR60_T50;
					break;
				case 70:
					theta = THETA_N9_WR70_T50;
					break;
				case 80:
					theta = THETA_N9_WR80_T50;
					break;
				case 90:
					theta = THETA_N9_WR90_T50;
					break;
				case 100:
					theta = THETA_N9_WR100_T50;
					break;					
				}
				break;
			}
			break;
		}
		return theta;
	}
}

