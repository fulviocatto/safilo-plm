import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import matrix.db.BusinessObject;
import matrix.db.BusinessObjectWithSelectList;
import matrix.db.Context;
import matrix.db.JPO;
import matrix.db.MatrixLogWriter;
import matrix.db.Program;
import matrix.util.MatrixException;
import matrix.util.StringList;

import com.matrixone.apps.domain.DomainObject;
import com.matrixone.apps.domain.util.FrameworkException;
import com.matrixone.apps.domain.util.FrameworkUtil;
import com.matrixone.apps.domain.util.MapList;
import com.matrixone.apps.domain.util.MqlUtil;
import com.matrixone.apps.domain.util.SetUtil;
import com.matrixone.apps.domain.util.mxBus;
import com.matrixone.apps.productline.ProductLineCommon;
import com.matrixone.apps.apr.util.DateUtil;

import org.apache.commons.lang.StringUtils;
public class SAFILO_ENOSAP_UtilsJPO_mxJPO implements SAFILO_ConstantsDefsJPO_mxJPO {

	private static boolean _debug = true;
	public static final int PROCEDURE_TIMEOUT = 3000;
	public static final int MAX_RETRY_NUM = 10;
	public static final int SELECT_MAX_NUM = 100000;

	// the connection string.
	// SVIL:
	//public static final String sDatabaseName = "enoviaSapDev";
	//public static final String connectionUrl = "jdbc:sqlserver://srv191pd:1433;databaseName="+sDatabaseName+";user=enovia;password=Enovia123";
	// TEST:
	public static final String sDatabaseName = "enoviaSap";
	public static final String connectionUrl = "jdbc:sqlserver://srv191pd:1433;databaseName=" + sDatabaseName + ";user=enovia;password=Enovia123";
	// PROD:
	//public static final String sDatabaseName = "enoviaSap";
	//public static final String connectionUrl = "jdbc:sqlserver://srv40pd:1433;databaseName="+sDatabaseName+";user=enoviaprod;password=Enovia123";

	public final static String SAFILO_SAP_COUNTER_FOR_COMM_COLOR = new String("SAFILO_ENOSAP_COMM_COLOR");
	public final static String SAFILO_SAP_COUNTER_FOR_COLOR_TECH = new String("SAFILO_ENOSAP_COLOR_TECH");
	public final static String SAFILO_SAP_COUNTER_FOR_EXT_DESCR = new String("SAFILO_ENOSAP_EXT_DESCR");
	public final static String SAFILO_SAP_COUNTER_FOR_DISTR_MKT = new String("SAFILO_ENOSAP_DISTR_MKT");
	public final static String SAFILO_SAP_COUNTER_FOR_LENS_TECH = new String("SAFILO_ENOSAP_LENS_TECH");
	public final static String SAFILO_SAP_COUNTER_FOR_SKU = new String("SAFILO_ENOSAP_SKU");
	public final static String SAFILO_SAP_COUNTER_FOR_STYLE = new String("SAFILO_ENOSAP_STYLE");
	public final static String SAFILO_SAP_DEF_LANGUAGE = new String("en");
	public final static String SAFILO_SAP_MAX_NUM_FOR_ID = new String("9999999999");
	public final static String SAFILO_SAP_UPDATES_SET = new String("TOSAP");
	public final static String SAFILO_SAP_UPDATES_DISTR_SET = new String("TOSAP_DISTR");
	public final static String SAFILO_SAP_UPDATES_DESCR_SET = new String("TOSAP_DESCR");

	public final static String SAFILO_SAP_ATTR_LENGTH_MAP = new String("SAFILO_ENOSAP_AttributesLength_Map");
	public final static String SAFILO_SAP_MANDATORY_COLOR_TECH_ATTRS = new String("SAFILO_ENOSAP_MANDATORY_COLOR_TECH_ATTRS");
	public final static String SAFILO_SAP_MANDATORY_LENSTECH_ATTRS = new String("SAFILO_ENOSAP_MANDATORY_LENSTECH_ATTRS");
	public final static String SAFILO_SAP_MANDATORY_MODEL_ATTRS = new String("SAFILO_ENOSAP_MODEL_MANDATORY_ATTRS");
	public final static String SAFILO_SAP_MANDATORY_SKU_ATTRS = new String("SAFILO_ENOSAP_MANDATORY_SKU_ATTRS");
	public final static String SAFILO_ENOSAP_MANDATORY_CANC_SKU_ATTRS = new String("SAFILO_ENOSAP_MANDATORY_SKU_CANCELLED_ATTRS");

	public final static String SAFILO_SAP_COLOR_TECH_ATTRS_MAPPING = new String("SAFILO_ENOSAP_COLOR_TECH_Attributes_Mapping");
	public final static String SAFILO_SAP_DISTR_MKT_ATTRS_MAPPING = new String("SAFILO_ENOSAP_DISTR_MKT_Attributes_Mapping");
	public final static String SAFILO_SAP_DISTR_MKT_DATE_ATTRS = new String("SAFILO_ENOSAP_DISTR_MKT_DateAttributes");
	public final static String SAFILO_SAP_EXTDESCR_ATTRS_MAPPING = new String("SAFILO_ENOSAP_EXTDESCR_Attributes_Mapping");
	public final static String SAFILO_SAP_EXTDESCR_TYPES_MAPPING = new String("SAFILO_ENOSAP_EXTDESCR_TextTypes_Mapping");
	public final static String SAFILO_SAP_LENSTECH_ATTRS_MAPPING = new String("SAFILO_ENOSAP_LENSTECH_Attributes_Mapping");
	public final static String SAFILO_SAP_LENSTECH_DATE_ATTRS = new String("SAFILO_ENOSAP_LENSTECH_DateAttributes");
	public final static String SAFILO_SAP_LENSTECH_FLAG_ATTRS = new String("SAFILO_ENOSAP_LENSTECH_BooleanAttributes");
	public final static String SAFILO_SAP_SKU_ATTRS_MAPPING = new String("SAFILO_ENOSAP_SKU_Attributes_Mapping");
	public final static String SAFILO_SAP_SKU_DATE_ATTRS = new String("SAFILO_ENOSAP_SKU_DateAttributes");
	public final static String SAFILO_SAP_SKU_FLAG_ATTRS = new String("SAFILO_ENOSAP_SKU_BooleanAttributes");
	public final static String SAFILO_SAP_STYLE_ATTRS_MAPPING = new String("SAFILO_ENOSAP_MODEL_Attributes_Mapping");
	public final static String SAFILO_SAP_STYLE_DATE_ATTRS = new String("SAFILO_ENOSAP_MODEL_DateAttributes");
	public final static String SAFILO_SAP_STYLE_FLAG_ATTRS = new String("SAFILO_ENOSAP_MODEL_BooleanAttributes");

	//return codes
	public final static String SAFILO_SAP_OK_000 = new String("000");
	public final static String SAFILO_SAP_READY_002 = new String("002");
	public final static String SAFILO_SAP_PROCESSING_003 = new String("003");
	public final static String SAFILO_SAP_WARNING_004 = new String("004");
	public final static String SAFILO_SAP_ERROR_008 = new String("008");
	public final static String SAFILO_SAP_CANCEL_012 = new String("012");
	public final static String SAFILO_SAP_CANCEL_016 = new String("016");
	public final static String SAFILO_SAP_WARNING_104 = new String("104");
	public final static String SAFILO_SAP_NOTREADY_108 = new String("108");
	public final static String SAFILO_SAP_UPDATE_ERROR_110 = new String("110");
	public final static String SAFILO_SAP_PROCESSED_100 = new String("100");
	public final static String SAFILO_SAP_IGNORE_101 = new String("101");

	public final static int SAFILO_SAP_SKU_2ND_DIM_LENGTH = 4;

	public static String getExceptionDetails(Exception exception) {
		// get the first 3 rows of the exception stack trace
		String sWhere = new String("");
		if (exception != null) {
			sWhere = "ERROR: " + exception.toString() + "\n";
			StackTraceElement[] stElArray = exception.getStackTrace();
			if (stElArray.length < 3) {
				for (int l = 0; l <= stElArray.length; l++) {
					StackTraceElement stEl = stElArray[l];
					sWhere += "Line " + stEl.getLineNumber() + " of " + stEl.getClassName() + "." + stEl.getMethodName() + "\n";
				}
			} else {
				for (int l = 0; l <= 3; l++) {
					StackTraceElement stEl = stElArray[l];
					sWhere += "Line " + stEl.getLineNumber() + " of " + stEl.getClassName() + "." + stEl.getMethodName() + "\n";
				}
			}
		} else {
			Throwable thDum = new Throwable();
			String sCallingClass = thDum.fillInStackTrace().getStackTrace()[1].getClassName();
			String sCallingMethod = thDum.fillInStackTrace().getStackTrace()[1].getMethodName();
			String sMsgErr = new String("");
			sMsgErr = "OPERAZIONE FALLITA: nessun dettaglio disponibile per (getExceptionDetails)";
			sWhere = sCallingClass + ":" + sCallingMethod + " - " + sMsgErr + "\n";
		}
		return sWhere;
	}


	/**
	 * @param sDate
	 * @param mxLog
	 * @return
	 */
	private static String getDateField(String sDate, BufferedWriter mxLog) {

		String sReturnVal = new String("");
		try {
			String sLocalDate = sDate.trim();
			//jpoLog(mxLog, "getDateField: sLocalDate1="+sLocalDate);
			if (!(null == sLocalDate || "".equals(sLocalDate) || "NULL".equalsIgnoreCase(sLocalDate))) {
				if (sLocalDate.contains(" ")) {
					StringList slOnlyDate = FrameworkUtil.split(sLocalDate, " ");
					if (slOnlyDate.size() > 1) {
						sLocalDate = (String) slOnlyDate.get(0);
					}
				}
				//jpoLog(mxLog, "getDateField: sLocalDate2="+sLocalDate);
				if (sLocalDate.contains("-")) {
					String attrFormat = "d-MMM-yyyy";
					SimpleDateFormat sdf = new SimpleDateFormat(attrFormat, Locale.US);
					try {
						Date theDate = sdf.parse(sLocalDate.trim());
						SimpleDateFormat fdata = new SimpleDateFormat("yyyyMMdd");
						sReturnVal = fdata.format(theDate);
						if (_debug) {
							jpoLog(mxLog, "getDateField: contains -, sReturnVal=<" + sReturnVal + ">");
						}
					} catch (ParseException ex) {
						String sExcDet = getExceptionDetails(ex);
						throw new MatrixException("Error while parsing date <" + sLocalDate + ">.\n" + sExcDet);
					}
				} else {
					StringList slDateInfo = FrameworkUtil.split(sLocalDate, "/");
					if (slDateInfo.size() == 3) {
						String sMonth = (String) slDateInfo.get(0);
						if (sMonth.length() == 1) {
							sMonth = "0" + sMonth;
						}
						String sDay = (String) slDateInfo.get(1);
						if (sDay.length() == 1) {
							sDay = "0" + sDay;
						}
						String sYear = (String) slDateInfo.get(2);
						// 2016-02-09: we will send the date with internal SAP format (aaaammdd)
						//sReturnVal   = sYear+"-"+sMonth+"-"+sDay;
						sReturnVal = sYear + sMonth + sDay;
						if (_debug) {
							jpoLog(mxLog, "getDateField: real date, sReturnVal=<" + sReturnVal + ">");
						}
					} else {
						if (_debug) {
							jpoLog(mxLog, "Exception in getDateField: <" + sLocalDate + ">, slDateInfo.size()=<" + slDateInfo.size() + ">. Must be 3");
						}
					}
				}
			}
		} catch (Exception e) {
			String sExcDetails = getExceptionDetails(e);
			if (_debug) {
				jpoLog(mxLog, "Exception in getDateField:  " + sExcDetails);
			}
			return sReturnVal;
		}
		return sReturnVal;
	}

	/**
	 * @return
	 */
	public static String getLogDate() {
		Calendar cal = Calendar.getInstance();
		Date dLogDate = cal.getTime();
		SimpleDateFormat fdata = new SimpleDateFormat("dd/M/yyyy HH:mm:ss");
		return fdata.format(dLogDate);
	}

	/**
	 * @param mxLog
	 * @param sMsg
	 */
	public static void jpoLog(BufferedWriter mxLog, String sMsg) {
		if (mxLog != null) {
			try {
				mxLog.write(getLogDate() + " - " + sMsg);
				mxLog.flush();
			} catch (IOException e) {
				System.out.println(getLogDate() + " - " + sMsg);
			}
		}
		if (_debug) {
			System.out.println(getLogDate() + " - " + sMsg);
		}
	}


	private String getCode(Context context, BufferedWriter mxLog, String sCountName)
			throws MatrixException {
		// initialize return variable
		String sNewCode = new String("NA");
		boolean isOk = false;

		try {
			// instantiate the counter object
			DomainObject doCounter = null;
			String sCounterId = new String("");
			try {
				BusinessObject boCounter = new BusinessObject(TYPE_ESERVICE_NUMBER_GENERATOR, sCountName, "", "eService Administration");
				boCounter.open(context);
				doCounter = DomainObject.newInstance(context, boCounter);
				sCounterId = doCounter.getId(context);
				boCounter.close(context);
			} catch (FrameworkException e) {
				String sErrMsg = new String("getCode - Error while reading the counter:\n" + getExceptionDetails(e));
				if (mxLog != null) {
					jpoLog(mxLog, sErrMsg);
				}
				throw new MatrixException(sErrMsg);
			}

			// check that the counter is not already reserved
			int iTimeOut = 0;
			String sMqlCmd = "print bus $1 select $2 dump;";
			try {
				//String isReserved = MqlUtil.mqlCommand(context, "print bus " + sCounterId + " select reserved dump;");
				String isReserved = MqlUtil.mqlCommand(context, sMqlCmd, new String[]{sCounterId, "reserved"});
				while (("TRUE".equals(isReserved.toUpperCase())) && (iTimeOut <= PROCEDURE_TIMEOUT)) {
					iTimeOut++;
					if (iTimeOut % 1000 == 0) {
						//jpoLog(mxLog, "Please wait..." + iTimeOut);
						//isReserved = MqlUtil.mqlCommand(context, "print bus " + sCounterId + " select reserved dump;");
						isReserved = MqlUtil.mqlCommand(context, sMqlCmd, new String[]{sCounterId, "reserved"});
					}
				}
			} catch (MatrixException e) {
				String sErrMsg = new String("getCode - Error while waiting for the unreserve of the counter:\n" + getExceptionDetails(e));
				if (mxLog != null) {
					jpoLog(mxLog, sErrMsg);
				}
				throw new MatrixException(sErrMsg);
			}
			String isReserved = new String("TRUE");
			try {
				//isReserved = MqlUtil.mqlCommand(context, "print bus " + sCounterId + " select reserved dump;");
				isReserved = MqlUtil.mqlCommand(context, sMqlCmd, new String[]{sCounterId, "reserved"});
			} catch (MatrixException e2) {
				String sErrMsg = new String("getCode - Error while verifying the counter:\n" + getExceptionDetails(e2));
				if (mxLog != null) {
					jpoLog(mxLog, sErrMsg);
				}
				throw new MatrixException(sErrMsg);
			}
			if ("FALSE".equals(isReserved.toUpperCase())) {
				// reserve the counter
				try {
					//MqlUtil.mqlCommand(context, "modify bus " + sCounterId + " reserve;");
					sMqlCmd = "modify bus $1 reserve;";
					MqlUtil.mqlCommand(context, sMqlCmd, new String[]{sCounterId});
				} catch (MatrixException e1) {
					String sErrMsg = new String("getCode - Error while reserving the counter:\n" + getExceptionDetails(e1));
					if (mxLog != null) {
						jpoLog(mxLog, sErrMsg);
					}
					throw new MatrixException(sErrMsg);
				}

				String sPresentVal = null;

				// loop until a valid code is found...
				// ...or the maximum of the retry is reached
				int iTry = 0;
				//int iCodeLength = 0;
				while ((!isOk) && (iTry <= MAX_RETRY_NUM)) {
					iTry++;
					try {
						sPresentVal = doCounter.getInfo(context, SELECT_ATTRIBUTE_ESERVICE_NEXT_NUMBER);
						//iCodeLength = sAttuale.length();
						//jpoLog(mxLog, "sAttuale         = " + sAttuale);
						//jpoLog(mxLog, "LUNGHEZZA CODICE = " + iCodeLength);

						// check that the number is in the valid range...
						String MAX_NUMBER_VAL = SAFILO_SAP_MAX_NUM_FOR_ID;
						if (Long.parseLong(sPresentVal) + 1 < Long.parseLong(MAX_NUMBER_VAL)) {
							// ...it is valid :)
							Long lNewValue = new Long(Long.parseLong(sPresentVal) + 1);
							String sNewValue = lNewValue.toString();
							//jpoLog(mxLog, "sNewValue        = " + sNewValue);
							try {
								doCounter.setAttributeValue(context, ATTRIBUTE_ESERVICE_NEXT_NUMBER, sNewValue);
							} catch (FrameworkException e) {
								String sErrMsg = new String("getCode - Error while updating the counter:\n" + getExceptionDetails(e));
								if (mxLog != null) {
									jpoLog(mxLog, sErrMsg);
								}
								throw new MatrixException(sErrMsg);
							}

							// seems to be OK...
							sNewCode = sNewValue;
							isOk = true;

							try {
								//MqlUtil.mqlCommand(context, "modify bus " + sCounterId + " unreserve;");
								sMqlCmd = "modify bus $1 unreserve;";
								MqlUtil.mqlCommand(context, sMqlCmd, new String[]{sCounterId});
							} catch (MatrixException e) {
								String sErrMsg = new String("getCode - Error while unreserving the counter:\n" + getExceptionDetails(e));
								if (mxLog != null) {
									jpoLog(mxLog, sErrMsg);
								}
								throw new MatrixException(sErrMsg);
							}
						} else {
							// ... not valid :( ...restart from the beginning
							sNewCode = "0";
							isOk = true;
						}
					} catch (FrameworkException e1) {
						String sErrMsg = new String("getCode - Error while reading the counter:\n" + getExceptionDetails(e1));
						if (mxLog != null) {
							jpoLog(mxLog, sErrMsg);
						}
						throw new MatrixException(sErrMsg);
					}
				}
			} else {
				// Counter still reserved...free it...
				try {
					//MqlUtil.mqlCommand(context, "modify bus " + sCounterId + " unreserve;");
					sMqlCmd = "modify bus $1 unreserve;";
					MqlUtil.mqlCommand(context, sMqlCmd, new String[]{sCounterId});
				} catch (MatrixException e) {
					e.printStackTrace();
				}

				// ...and log that the operation must be repeated
				String sErrMsg = new String("getCode - Impossible to continue: counter still reserved. The operation must be repeated");
				if (mxLog != null) {
					jpoLog(mxLog, sErrMsg);
				}
				throw new MatrixException(sErrMsg);
			}
		} catch (NumberFormatException e) {
			// ...ohi ohi...
			sNewCode = new String("");
			String sErrMsg = new String("getCode - NumberFormatExcetpion:\n" + getExceptionDetails(e));
			if (mxLog != null) {
				jpoLog(mxLog, sErrMsg);
			}
			throw new MatrixException(sErrMsg);
		} catch (MatrixException e) {
			throw new MatrixException(getExceptionDetails(e));
		} finally {
			//ContextUtil.popContext(context);
		}

		return sNewCode;
	}

	/**
	 * <BR>
	 * <code>getHashMapFromMapProgram</code> <BR>
	 * Returns in an HashMap object the values found in the uncommented lines of a mapProgram identified by the
	 * sPrgName parameter field. Note: no assumptions are made upon the data nature <BR>
	 *
	 * @param context <code>Context</code> the matrix context object
	 * @param args    <code>String</code> the mapProgram used to load the returned HashMap
	 * @return <code>HashMap</code>
	 * @throws Exception
	 */
	public static Object getHashMapFromMapProgram(Context context, String sPrgName) throws Exception {

		// return variable
		HashMap hmMap = new HashMap();

		Program pDataMap = null;
		String sDataMap = null;

		pDataMap = new Program(sPrgName, false, false, false, false);
		pDataMap.open(context);
		sDataMap = pDataMap.getCode(context);

		if (sDataMap != null) {
			StringList slRows = FrameworkUtil.split(sDataMap, "\n");
			for (int idx = 0; idx < slRows.size(); idx++) {
				String sRow = (String) slRows.get(idx);
				if (!((sRow == null) || ("".equals(sRow)) || (sRow.startsWith("#")))) {
					StringList slRowInfo = FrameworkUtil.split(sRow, "|");
					String sKey = (String) slRowInfo.get(0);
					String sValue = (String) slRowInfo.get(1);

					if (!((sValue == null) || ("".equals(sValue)) || (sValue.startsWith("#")))) {
						hmMap.put(sKey, sValue);
					}
				}
			}
		}

		return hmMap;
	}

	/**
	 * <BR>
	 * <code>getStringListFromMapProgram</code> Returns in a StringList the uncommented lines of a map program <BR>
	 *
	 * @param context  <code>Context</code> the matrix context object
	 * @param sPrgName <code>String</code> the map program name
	 * @return <code>StringList</code>
	 * @throws Exception
	 */
	public static Object getStringListFromMapProgram(Context context, String sPrgName) throws Exception {

		StringList slReturn = new StringList();

		// no empty list allowed
		//slReturn.add("DUMMY");

		Program pDataMap = null;
		String sDataMap = null;

		// scan the map program
		pDataMap = new Program(sPrgName, false, false, false, false);
		pDataMap.open(context);
		sDataMap = pDataMap.getCode(context);

		if (sDataMap != null) {
			StringList slRows = FrameworkUtil.split(sDataMap, "\n");
			for (int idx = 0; idx < slRows.size(); idx++) {
				String sRow = (String) slRows.get(idx);
				if (!(("".equals(sRow.trim())) || (sRow == null) || (sRow.startsWith("#")))) {
					if (sRow.indexOf("|") > 0) {
						StringList slTemp = FrameworkUtil.split(sRow, "|");
						String sKey = (String) slTemp.get(0);
						slReturn.add(sKey.trim());
					} else {
						slReturn.add(sRow.trim());
					}
				}
			}
			slReturn.sort();
		}
		return slReturn;
	}

	public int checkModelMandatoryAttributesForSAP(Context context, String[] args)
			throws Exception {
		String sObjectId = args[0];
		String sMapProgram = SAFILO_SAP_MANDATORY_MODEL_ATTRS;
		BufferedWriter mxLog = null;
		if (_debug) {
			MatrixLogWriter mtxw = new MatrixLogWriter(context, "CheckModelMandatoryAttrs.log", "LOGWRITER", false);
			mxLog = new BufferedWriter(mtxw);
			jpoLog(mxLog, "Chekcing mandatory attributes on Model " + sObjectId);
		}
		String sMissingAttr = checkMandatoryAttributesForSAP(context, sObjectId, sMapProgram, mxLog);
		if (_debug) {
			mxLog.close();
		}
		int iRetVal = 1;
		if ("".equals(sMissingAttr)) {
			iRetVal = 0;
		} else {
			throw new MatrixException(sMissingAttr);
		}
		return iRetVal;
	}

	public int checkSkuMandatoryAttributesForSAP(Context context, String[] args)
			throws Exception {
		int iRetVal = 1;
		String sObjectId = args[0];
		String sMapProgram = SAFILO_SAP_MANDATORY_SKU_ATTRS;
		BufferedWriter mxLog = null;
		if (_debug) {
			MatrixLogWriter mtxw = new MatrixLogWriter(context, "CheckSkuMandatoryAttrs.log", "LOGWRITER", false);
			mxLog = new BufferedWriter(mtxw);
		}
		Calendar cal = Calendar.getInstance();
		Date dayDate = cal.getTime();
		SimpleDateFormat fdata = new SimpleDateFormat("yyyyMMdd");
		String sDate = fdata.format(dayDate);
		if (_debug) {
			jpoLog(mxLog, "Checking mandatory attributes on SKU " + sObjectId);
		}
		// verify the related model has been sent to SAP
		DomainObject doSku = DomainObject.newInstance(context, sObjectId);
		StringList slSkuSel = new StringList(4);
		slSkuSel.add(SELECT_NAME);
		slSkuSel.add(SELECT_MODEL_CODE_FROM_SKU);
		slSkuSel.add(SELECT_MODEL_CURRENT_FROM_SKU);
		slSkuSel.add(SELECT_MODEL_NAME_FROM_SKU);
		Map mSkuInfo = doSku.getInfo(context, slSkuSel);
		String sModelName = (String) mSkuInfo.get(SELECT_MODEL_NAME_FROM_SKU);
		String sModelCode = (String) mSkuInfo.get(SELECT_MODEL_CODE_FROM_SKU);
		String sModelCurr = (String) mSkuInfo.get(SELECT_MODEL_CURRENT_FROM_SKU);
		// verify the model current state
		if (STATE_SKETCH.equals(sModelCurr) || STATE_CAD.equals(sModelCurr) || STATE_PROTOTYPE.equals(sModelCurr) || "WAIT_APPROVAL".equals(sModelCurr)) {
			String sMsgErr = "SKU activation not allowed: the related style (" + sModelName + ") is in <" + sModelCurr + "> state.";
			emxContextUtil_mxJPO.mqlNotice(context, sMsgErr);
			if (_debug) {
				jpoLog(mxLog, sMsgErr);
			}
			createErrorDetail(context, mxLog, "NA", "ES_SKU", sDate, SAFILO_SAP_NOTREADY_108, sMsgErr, "E");
		} else {
			// verify the model code
			if (!sModelName.equals(sModelCode)) {
				String sMsgErr = "SKU activation not allowed: the related style (" + sModelName + ") has not been renamed in <" + sModelCode + "> yet.";
				sMsgErr += "\nPlease wait until the style is renamed and repeat the activation";
				emxContextUtil_mxJPO.mqlNotice(context, sMsgErr);
				if (_debug) {
					jpoLog(mxLog, sMsgErr);
				}
				createErrorDetail(context, mxLog, "NA", "ES_SKU", sDate, SAFILO_SAP_NOTREADY_108, sMsgErr, "E");
			} else {
				String sSkuName = (String) mSkuInfo.get(SELECT_NAME);
				if (!sSkuName.startsWith(sModelName)) {
					String sMsgErr = "SKU activation not allowed: the SKU code must start with <" + sModelName + ">.";
					sMsgErr += "\nPlease wait until the SKU is renamed and repeat the activation";
					emxContextUtil_mxJPO.mqlNotice(context, sMsgErr);
					if (_debug) {
						jpoLog(mxLog, sMsgErr);
					}
					createErrorDetail(context, mxLog, "NA", "ES_SKU", sDate, SAFILO_SAP_NOTREADY_108, sMsgErr, "E");
				} else {
					String sMissingAttr = checkMandatoryAttributesForSAP(context, sObjectId, sMapProgram, mxLog);
					if ("".equals(sMissingAttr)) {
						iRetVal = 0;
					} else {
						String sMsgErr = "Mandatory attributes are not valorized:\n" + sMissingAttr + "\nSend to SAP cancelled.";
						emxContextUtil_mxJPO.mqlNotice(context, sMsgErr);
						if (_debug) {
							jpoLog(mxLog, sMsgErr);
						}
						createErrorDetail(context, mxLog, "NA", "ES_SKU", sDate, SAFILO_SAP_NOTREADY_108, sMsgErr, "E");
					}
				}
			}
		}
		if (_debug) {
			mxLog.close();
		}
		return iRetVal;
	}

	public int checkLensTechMandatoryAttributesForSAP(Context context, String[] args)
			throws Exception {
		String sObjectId = args[0];
		String sMapProgram = SAFILO_SAP_MANDATORY_LENSTECH_ATTRS;
		BufferedWriter mxLog = null;
		if (_debug) {
			MatrixLogWriter mtxw = new MatrixLogWriter(context, "CheckLensTechMandatoryAttrs.log", "LOGWRITER", false);
			mxLog = new BufferedWriter(mtxw);
			jpoLog(mxLog, "Chekcing mandatory attributes on LensTech " + sObjectId);
		}
		String sMissingAttr = checkMandatoryAttributesForSAP(context, sObjectId, sMapProgram, mxLog);
		int iRetVal = 1;
		if ("".equals(sMissingAttr)) {
			iRetVal = 0;
		}
		if (_debug) {
			mxLog.close();
		}
		return iRetVal;
	}

	/**
	 * @param context
	 * @param args
	 * @return
	 * @throws MatrixException
	 */
	public int sendExtDescrToSAP(Context context, String sObjectId, boolean useTransaction) throws Exception {

		int iRetCode = 0;
		String sId = new String("");
		//String sJobId          = new String("");
		BufferedWriter mxLog = null;
		boolean isTrans = false;
		String sCountName = SAFILO_SAP_COUNTER_FOR_EXT_DESCR;
		String sTableName = "ES_EXTENDED_DESCR";

		try {
			if (_debug) {
				MatrixLogWriter mtxw = new MatrixLogWriter(context, "ExtDescrTOSAP.log", "LOGWRITER", false);
				mxLog = new BufferedWriter(mtxw);
			}

			emxContextUtil_mxJPO ShadowAgent = new emxContextUtil_mxJPO(context, null);
			ShadowAgent.pushContext(context, null);

			// get the time stamp
			String sTimeStamp = getTimestamp();

			Calendar cal = Calendar.getInstance();
			Date dayDate = cal.getTime();
			SimpleDateFormat fdata = new SimpleDateFormat("yyyyMMdd");
			String sDate = fdata.format(dayDate);
			if (_debug) {
				jpoLog(mxLog, sDate + " sendToSAP (ExtDescr): started on bus " + sObjectId + " with timestamp " + sTimeStamp);
			}

			// retrieve object information and insert the records in the ENOSAP tables
			// get the attributes to check
			String sAttrsMappingMap = SAFILO_SAP_EXTDESCR_ATTRS_MAPPING;
			StringList slAttrs = (StringList) getStringListFromMapProgram(context, sAttrsMappingMap);
			//HashMap    hmAttrsMap  = (HashMap)    getHashMapFromMapProgram(context, sAttrsMappingMap);
			HashMap hmTypesMap = (HashMap) getHashMapFromMapProgram(context, SAFILO_SAP_EXTDESCR_TYPES_MAPPING);

			// get the attributes values
			DomainObject domObject = new DomainObject(sObjectId);
			Map mAttrsVal = domObject.getInfo(context, slAttrs);
			String sCode = (String) domObject.getInfo(context, SELECT_ATTRIBUTE_SAFILO_MODELCODE);
			String sSkuCode = (String) domObject.getInfo(context, SELECT_ATTRIBUTE_SAFILO_SKUCODE);
			if (_debug) {
				jpoLog(mxLog, sDate + " sendToSAP (ExtDescr):\n\t" + SELECT_ATTRIBUTE_SAFILO_MODELCODE + "=" + sCode + "\n\t" + SELECT_ATTRIBUTE_SAFILO_SKUCODE + "=" + sSkuCode);
			}

			// define the hash map of the attributes, one record for each description type (BTB, BTC, SHIP, INT)
			//        attribute[SAFILO_ShippingNotes]
			//        attribute[SAFILO_ProductDescriptionForB2B]
			//        attribute[SAFILO_ProductDescriptionForB2C]
			//        description
			for (int i = 0; i < slAttrs.size(); i++) {
				String sAttrName = (String) slAttrs.get(i);
				String sText = (String) mAttrsVal.get(sAttrName);
				if ((sText != null) && (!"".equals(sText)) && (!"null".equals(sText))) {
					String sTextType = (String) hmTypesMap.get(sAttrName);
					isTrans = createExtDescr(context,
							false,
							mxLog,
							sCountName,
							sTableName,
							sTimeStamp,
							sDate,
							sCode,
							sSkuCode,
							sText,
							sTextType,
							useTransaction);
				}
			}
			if (_debug) {
				mxLog.close();
			}
		} catch (Exception e) {
			// log the error
			String sExcDetails = "SAFILO_SAP_SendExtDescrToSAP execution error for BUS with ID " + sObjectId;
			sExcDetails += "\n(SAFILO_SAP with ID=" + sId + " and ID=" + sId + "):\n";
			sExcDetails += getExceptionDetails(e);

			// errors: rollback the transaction (?)
			try {
				//MqlUtil.mqlCommand(context, "abort transaction");
				if (isTrans) {
					MqlUtil.mqlCommand(context, "abort transaction", new String[]{});
				}
				sExcDetails += "\n=====\nSAFILO_SAP_SendToSAP rollback OK";
			} catch (Exception e1) {
				// log the error
				sExcDetails += "\n=====\nSAFILO_SAP_SendToSAP rollback error: " + getExceptionDetails(e1);
			}

			if (_debug) {
				jpoLog(mxLog, "sendToSAP s'e' rrroottttt!\n" + sExcDetails);
				mxLog.close();
			}
			throw new MatrixException(getLogDate() + " - " + "sendToSAP: " + sExcDetails);
		}
		return iRetCode;
	}

	public String getTimestamp() {
		return getTimestamp(true);
	}

	public String getTimestamp(boolean bFormat) {
		//    Calendar cal = Calendar.getInstance();
		//    Long timestamp = cal.getTimeInMillis();
		//    String sTimeStamp = timestamp.toString();
		Calendar cal = Calendar.getInstance();
		Date dayDate = cal.getTime();
		SimpleDateFormat fdata = new SimpleDateFormat("yyyyMMddHHmmssS");
		if (bFormat) {
			fdata = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		}
		String sTimeStamp = fdata.format(dayDate);
		return sTimeStamp;
	}


	public boolean createExtDescr(
			Context context,
			boolean isNew,
			BufferedWriter mxLog,
			String sCountName,
			String sTableName,
			String sTimeStamp,
			String sDate,
			String sCode,
			String sSkuCode,
			String sText,
			String sTextType,
			boolean useTransaction)
			throws MatrixException {
		boolean isTrans = false;
		// set the attributes
		HashMap hmAdapletAttr = new HashMap(12);
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_JOB_USER, context.getUser());
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_READY_002);
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_STATUS_DATE, sDate);
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_MSG_TABLE, sTableName);
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_TIMESTAMP, sTimeStamp);
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_SENDER_SYSTEM, "ENO");
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_RECEIVER_SYSTEM, "SAP");
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_MATNR, sCode);
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_SKU_CODE, sSkuCode);
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_TEXT_LANGUAGE, "EN");
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_TEXT, sText.toUpperCase());
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_TEXT_TYPE, sTextType.toUpperCase());
		if (isNew) {
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE, "C");
		} else {
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE, "M");
		}
		if (_debug) {
			//jpoLog(mxLog, "Attributes =<"+ hmAdapletAttr + ">");
		}

		// start transaction
		try {
			if (useTransaction) {
				MqlUtil.mqlCommand(context, "start transaction", new String[]{});
				isTrans = true;
			}

			// get the ID
			String sExtDescrId = getCode(context, mxLog, sCountName);
			if (_debug) {
				jpoLog(mxLog, "Creating new EXT.DESCR. with code=<" + sExtDescrId + "> for table " + sTableName);
			}

			// create the adaplet object
			BusinessObject boToSap = mxBus.create(context, "SAFILO_ENOSAP_" + sTableName, sExtDescrId, "", POLICY_SAFILO_ENOSAP, VAULT_ENOSAP);
			DomainObject doToSap = new DomainObject(boToSap);
			doToSap.open(context);
			doToSap.setAttributeValues(context, hmAdapletAttr);
			doToSap.close(context);

			// no errors: commit the transaction
			//MqlUtil.mqlCommand(context, "commit transaction");
			if (useTransaction) {
				MqlUtil.mqlCommand(context, "commit transaction", new String[]{});
			}
		} catch (Exception e) {
			String sErrMsg = getExceptionDetails(e);
			if (useTransaction) {
				if (isTrans) {
					try {
						MqlUtil.mqlCommand(context, "abort transaction", new String[]{});
					} catch (FrameworkException e1) {
						sErrMsg += "\n ROLLBACK ERROR:\n" + getExceptionDetails(e1);
					}
				}
			}
			throw new MatrixException(sErrMsg);
		}
		return isTrans;
	}


	public String checkMandatoryAttributesForSAP(Context context, String sObjectId, String sMapProgram, BufferedWriter mxLog)
			throws Exception {
		String sMissingAttribute = "";

		DomainObject domObject = new DomainObject(sObjectId);
		String sName = domObject.getInfo(context, "name");

		// get the attributes to check
		StringList slMandatoryAttr = (StringList) getStringListFromMapProgram(context, sMapProgram);
		if (_debug) {
			//jpoLog(mxLog, "Attributes to check: " + slMandatoryAttr);
		}

		// get the STYLE material type
		String sMatType = domObject.getInfo(context, SELECT_MATERIAL_TYPE_FROM_SKU);
		if ("ZFP1".equals(sMatType)) {
			slMandatoryAttr.add(SELECT_ATTRIBUTE_SAFILO_SKUTEMPLELENGTHACTUAL);
		} else if ("ZFP2".equals(sMatType)) {
			String sProductType = domObject.getInfo(context, SELECT_PRODUCT_TYPE_FROM_SKU);
			char cPrdType = sProductType.charAt(0);
			if (cPrdType != '6' && cPrdType != '7') {
				slMandatoryAttr.add(SELECT_ATTRIBUTE_SAFILO_SKUTEMPLELENGTHACTUAL);
			}
		} else if ("ZFP3".equals(sMatType)) {
			slMandatoryAttr.add(SELECT_ATTRIBUTE_SAFILO_SKUTEMPLELENGTHACTUAL);
			slMandatoryAttr.add(SELECT_ATTRIBUTE_SAFILO_FOCALPOWER);
		}

		// get the attributes values
		Map mMandAttrVal = domObject.getInfo(context, slMandatoryAttr);
		if (_debug) {
			//jpoLog(mxLog, "Attributes values: " + mMandAttrVal);
		}

		// check the attributes
		for (int i = 0; i < slMandatoryAttr.size(); i++) {
			String sAttrSel = (String) slMandatoryAttr.get(i);
			String sAttrVal = (String) mMandAttrVal.get(sAttrSel);
			if (sAttrVal == null) {
				if ("".equals(sMissingAttribute)) {
					sMissingAttribute = sAttrSel;
				} else {
					sMissingAttribute += "\n" + sAttrSel;
				}
			} else {
				if ("".equals(sAttrVal.trim()) || "-".equals(sAttrVal.trim())) {
					if ("".equals(sMissingAttribute)) {
						sMissingAttribute = sAttrSel;
					} else {
						sMissingAttribute += "\n" + sAttrSel;
					}
				}
			}
		}
		if (!"".equals(sMissingAttribute)) {
			// String sErrorMessage =
			// FrameworkUtil.i18nStringNow("emxIEFDesignCenter.Error.SearchFailed",request.getHeader("Accept-Language"));
			String sErrorMessage = "Missing Attributes for (" + sName + "): ";
			sErrorMessage += sMissingAttribute;
			emxContextUtil_mxJPO.mqlNotice(context, sErrorMessage);
		}
		return sMissingAttribute;
	}

	/**
	 * @param context
	 * @param args
	 * @return
	 * @throws MatrixException
	 */
	public int sendLensTechToSAP(Context context, String[] args) throws Exception {
		int iRetCode = 0;

		String sObjectId = args[0];
		DomainObject doBus = DomainObject.newInstance(context, sObjectId);
		String sBusType = doBus.getInfo(context, SELECT_TYPE);
		if (TYPE_SAFILO_LENSCOLOR_TECHNICAL.equals(sBusType)) {
			iRetCode = sendToSAP(context, sObjectId, SAFILO_SAP_MANDATORY_LENSTECH_ATTRS, SAFILO_SAP_COUNTER_FOR_LENS_TECH, SAFILO_SAP_LENSTECH_ATTRS_MAPPING, SAFILO_SAP_LENSTECH_DATE_ATTRS, SAFILO_SAP_LENSTECH_FLAG_ATTRS, "ES_LENTECH");
		}
		return iRetCode;
	}

	/**
	 * @param context
	 * @param sSkuId
	 * @throws Exception
	 */
	private int getAndSendLensTechForSku(
			Context context,
			String sSkuId) throws Exception {

		int iRetCode = 0;
		//    StringList selectStmts = new StringList(4);
		//    selectStmts.addElement(SELECT_ID);
		//    selectStmts.addElement(SELECT_TYPE);
		//    selectStmts.addElement(SELECT_NAME);
		//    selectStmts.addElement(SELECT_LEVEL);
		//
		//    StringList selectRelStmts = new StringList(4);
		//    selectRelStmts.addElement(SELECT_RELATIONSHIP_ID);
		//    selectRelStmts.addElement(SELECT_LENTECH_ID_FROM_LENS_LE);

		// get the lens tech id
		DomainObject domSku = DomainObject.newInstance(context, sSkuId);
		String sProductId = (String) domSku.getInfo(context, SELECT_PRODUCT_FROM_SKU);
		DomainObject domProduct = new DomainObject(sProductId);
		String sProdType = (String) domProduct.getInfo(context, SELECT_TYPE);
		if (TYPE_SAFILO_SUNGLASS.equals(sProdType)) {
			//      String sWhere="";
			//      MapList mlPrdEbom = domProduct.getRelatedObjects(context, RELATIONSHIP_PRODUCT_EBOM, "*", selectStmts, selectRelStmts, false, true, (short)1, sWhere, "", 0);
			//      MapList mlEbom = new MapList();
			//
			//      //PRODUCTEBOM --> only one Part connected to a Product
			//      for (int i=0; i< mlPrdEbom.size(); i++)
			//      {
			//        Hashtable htPrdEbom = (Hashtable)(mlPrdEbom.get(i));
			//        String sPartId = (String)htPrdEbom.get(DomainConstants.SELECT_ID);
			//        DomainObject domPart = new DomainObject(sPartId);
			//
			//        // looking for LE_Lens object
			//        mlEbom = domPart.getRelatedObjects(context, RELATIONSHIP_EBOM, TYPE_SAFILO_LE_LENS, selectStmts, selectRelStmts, false, true, (short)1, sWhere, "", 0);
			//        for (int y=0; y< mlEbom.size(); y++)
			//        {
			//          Hashtable htEbom = (Hashtable)(mlEbom.get(y));
			//          StringList slTechLensId = (StringList)htEbom.get(SELECT_LENTECH_ID_FROM_LENS_LE);
			//          for ( int l = 0; l < slTechLensId.size(); l++ )
			//          {
			//            String sLenTechId = (String)slTechLensId.get(l);
			//            int iRetCode = sendToSAP(context, sLenTechId, SAFILO_SAP_MANDATORY_LENSTECH_ATTRS, SAFILO_SAP_COUNTER_FOR_LENS_TECH, SAFILO_SAP_LENSTECH_ATTRS_MAPPING, SAFILO_SAP_LENSTECH_DATE_ATTRS, SAFILO_SAP_LENSTECH_FLAG_ATTRS, "ES_LENTECH");
			//          }
			//        }
			//      }
			String sLenTechId = (String) domSku.getInfo(context, SELECT_ATTRIBUTE_SAFILO_TECHNICALCOLOR);
			iRetCode = sendToSAP(context, sLenTechId, SAFILO_SAP_MANDATORY_LENSTECH_ATTRS, SAFILO_SAP_COUNTER_FOR_LENS_TECH, SAFILO_SAP_LENSTECH_ATTRS_MAPPING, SAFILO_SAP_LENSTECH_DATE_ATTRS, SAFILO_SAP_LENSTECH_FLAG_ATTRS, "ES_LENTECH");
		}
		return iRetCode;

	}


	/**
	 * @param context
	 * @param args
	 * @return
	 * @throws MatrixException
	 */
	public int sendSkuToSAP(Context context, String[] args) throws Exception {
		int iRetCode = 1;
		String sObjectId = args[0];
		getAndSendLensTechForSku(context, sObjectId);
		iRetCode = sendToSAP(context, sObjectId, SAFILO_SAP_MANDATORY_SKU_ATTRS, SAFILO_SAP_COUNTER_FOR_SKU, SAFILO_SAP_SKU_ATTRS_MAPPING, SAFILO_SAP_SKU_DATE_ATTRS, SAFILO_SAP_SKU_FLAG_ATTRS, "ES_SKU");
		if (iRetCode == 0) {
			// iRetCode = sendCommColorToSAP(context, sObjectId, true);
		}
		return iRetCode;
	}

	/**
	 * @param context
	 * @param args
	 * @return
	 * @throws MatrixException
	 */
	public int sendSkuCancelledToSAP(Context context, String[] args) throws Exception {
		int iRetCode = 1;
		String sObjectId = args[0];
		getAndSendLensTechForSku(context, sObjectId);
		iRetCode = sendToSAP(context, sObjectId, SAFILO_ENOSAP_MANDATORY_CANC_SKU_ATTRS, SAFILO_SAP_COUNTER_FOR_SKU, SAFILO_SAP_SKU_ATTRS_MAPPING, SAFILO_SAP_SKU_DATE_ATTRS, SAFILO_SAP_SKU_FLAG_ATTRS, "ES_SKU");
		if (iRetCode == 0) {
			//  iRetCode = sendCommColorToSAP(context, sObjectId, true);
		}
		return iRetCode;
	}

	/**
	 * @param context
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public int sendStyleToSAP(Context context, String[] args)
			throws Exception {
		BufferedWriter mxLog = null;
		String sObjectId = args[0];
		if (_debug) {
			MatrixLogWriter mtxw = new MatrixLogWriter(context, "sendStyleToSAP.log", "LOGWRITER", false);
			mxLog = new BufferedWriter(mtxw);
			jpoLog(mxLog, "Invoking sendToSAP (STYLE) for bus " + sObjectId);
			mxLog.close();
		}
		return sendToSAP(context, sObjectId, SAFILO_SAP_MANDATORY_MODEL_ATTRS, SAFILO_SAP_COUNTER_FOR_STYLE, SAFILO_SAP_STYLE_ATTRS_MAPPING, SAFILO_SAP_STYLE_DATE_ATTRS, SAFILO_SAP_STYLE_FLAG_ATTRS, "ES_STYLE");
	}

	/**
	 * @param context
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public int sendSkuBomToSAP(Context context, String[] args)
			throws Exception {

		String sObjectId = args[0];
		int iRetCode = 0;
		boolean bIsError = false;
		String sSkuName = new String("");

		// navigate the model
		StringList slBusSel = new StringList(3);
		slBusSel.add(SELECT_CURRENT);
		slBusSel.add(SELECT_ID);
		slBusSel.add(SELECT_NAME);
		try {
			DomainObject doProto = DomainObject.newInstance(context, sObjectId);
			MapList mlRelatedSku = doProto.getRelatedObjects(context, RELATIONSHIP_SAFILO_PRODUCTS_SKU, TYPE_SAFILO_SKU, slBusSel, null, true, true, (short) 1, "", "", 0);
			Iterator itrSku = mlRelatedSku.iterator();
			while (itrSku.hasNext()) {
				Map mapStr = (Map) itrSku.next();
				String sSkuCurr = (String) mapStr.get(SELECT_CURRENT);
				sSkuName = (String) mapStr.get(SELECT_NAME);
				if ("ACTIVE".equalsIgnoreCase(sSkuCurr)) {
					String sSkuId = (String) mapStr.get(SELECT_ID);
					if (_debug) {
						MatrixLogWriter mtxw = new MatrixLogWriter(context, "sendSkuBomToSAP.log", "LOGWRITER", false);
						BufferedWriter mxLog = new BufferedWriter(mtxw);
						jpoLog(mxLog, "Invoking sendSkuBomToSAP for bus " + sSkuId);
						mxLog.close();
					}
					iRetCode = sendCommColorToSAP(context, sSkuId, true);
					if (iRetCode != 0) {
						bIsError = true;
					}

				}
			}
		} catch (Exception e) {
			// get Exception details
			e.printStackTrace();
			String sExcDetails = getExceptionDetails(e);
			throw new MatrixException("Error while sending BOM update for SKU " + sSkuName + ":\n" + sExcDetails);
		}

		if (bIsError) {
			iRetCode = 1;
		}

		return iRetCode;
	}

	/**
	 * @param context
	 * @param args
	 * @return
	 * @throws MatrixException
	 */
	public int sendExtDescrToSAP(Context context, String[] args) throws Exception {
		String sObjectId = args[0];
		return sendExtDescrToSAP(context, sObjectId, true);
	}

	/**
	 * @param context
	 * @param args
	 * @return
	 * @throws MatrixException
	 */
	public int sendDistrMarketToSAP(Context context, String[] args) throws Exception {
		String sObjectId = args[0];
		return sendDistrMktToSAP(context, sObjectId, false, true);
	}

	/**
	 * @param context
	 * @param args
	 * @return
	 * @throws MatrixException
	 */
	public int sendUpdatesToSAP(Context context, String[] args) throws Exception {

		int iRetCode = 0;
		BufferedWriter mxLog = null;
		try {
			if (_debug) {
				MatrixLogWriter mtxw = new MatrixLogWriter(context, "TO_SAP.log", "LOGWRITER", false);
				//mxLog = new BufferedWriter(new MatrixLogWriter(context));
				mxLog = new BufferedWriter(mtxw);
				jpoLog(mxLog, "sendUpdatesToSAP: Started ");
			}

			// set creator context
			emxContextUtil_mxJPO ShadowAgent = new emxContextUtil_mxJPO(context, null);
			ShadowAgent.pushContext(context, null);

			if (_debug) {
				jpoLog(mxLog, "sendUpdatesToSAP: context setted");
			}

			iRetCode = sendSetMembers(context, SAFILO_SAP_UPDATES_SET, mxLog);
			iRetCode = sendSetMembers(context, SAFILO_SAP_UPDATES_DISTR_SET, mxLog);
			iRetCode = sendSetMembers(context, SAFILO_SAP_UPDATES_DESCR_SET, mxLog);

			ShadowAgent.popContext(context, null);
			if (_debug) {
				jpoLog(mxLog, "sendUpdatesToSAP: finished" + iRetCode);
				mxLog.close();
			}
			return iRetCode;

		} catch (Exception e) {
			String sExcDetails = getExceptionDetails(e);
			if (_debug) {
				jpoLog(mxLog, "sendUpdatesToSAP s'e' roottt! " + sExcDetails);
				mxLog.close();
			}
			throw new MatrixException(getLogDate() + " - " + "sendUpdatesToSAP: " + sExcDetails);
		}
	}


	/**
	 * @param context
	 * @param iRetCode
	 * @param mxLog
	 * @return
	 * @throws Exception
	 */
	private int sendSetMembers(Context context, String sSetName, BufferedWriter mxLog)
			throws Exception {
		int iRetCode = 0;
		// check if the TOSAP set exists
		if (SetUtil.exists(context, sSetName)) {
			StringList slBusSelects = new StringList(9);
			slBusSelects.add(SELECT_ID);
			slBusSelects.add(SELECT_TYPE);
			slBusSelects.add(SELECT_NAME);
			slBusSelects.add(SELECT_CURRENT);
			slBusSelects.add(SELECT_ATTRIBUTE_SAFILO_MODELCODE);
			slBusSelects.add(SELECT_KINDOF_EYEWEAR);
			slBusSelects.add(SELECT_KINDOF_SAFILO_SKU);
			slBusSelects.add(SELECT_KINDOF_SAFILO_LENS_TECH);
			slBusSelects.add(SELECT_ATTRIBUTE_SAFILO_CROSSPLANTSTATUS);

			StringList slUpdated = new StringList();
			String sBusId = new String("");
			String sTableName = new String("");
			MapList mlBusToUpdate = SetUtil.getMembers(context, sSetName, slBusSelects);
			Iterator itrSet = mlBusToUpdate.iterator();

			while (itrSet.hasNext()) {
				try {
					Map mapBus = (Map) itrSet.next();
					String isKindOfEyewear = (String) mapBus.get(SELECT_KINDOF_EYEWEAR);
					String isKindOfSku = (String) mapBus.get(SELECT_KINDOF_SAFILO_SKU);
					String sBusType = (String) mapBus.get(SELECT_TYPE);
					sBusId = (String) mapBus.get(SELECT_ID);
					if ("TRUE".equalsIgnoreCase(isKindOfEyewear)) {
						String sStyleName = (String) mapBus.get(SELECT_NAME);
						String sStyleCode = (String) mapBus.get(SELECT_ATTRIBUTE_SAFILO_MODELCODE);
						if (sStyleName.equals(sStyleCode)) {
							String sStyleCurrent = (String) mapBus.get(SELECT_CURRENT);
							String sStyleCrossPlant = (String) mapBus.get(SELECT_ATTRIBUTE_SAFILO_CROSSPLANTSTATUS);
							if ("ZZ".equalsIgnoreCase(sStyleCrossPlant) && !"CANCELLED".equalsIgnoreCase(sStyleCurrent)) {
								if (_debug) {
									jpoLog(mxLog, "Skipping sendToSAP (STYLE) for bus " + sStyleName + " (" + sBusId + "): CrossPlantStatus=ZZ");
								}
							} else {
								if (SAFILO_SAP_UPDATES_SET.equals(sSetName)) {
									if (_debug) {
										jpoLog(mxLog, "Invoking sendToSAP (STYLE) for bus " + sStyleName + " (" + sBusId + ")");
									}
									sTableName = "ES_STYLE";
									iRetCode = sendToSAP(context, sBusId, SAFILO_SAP_MANDATORY_MODEL_ATTRS, SAFILO_SAP_COUNTER_FOR_STYLE, SAFILO_SAP_STYLE_ATTRS_MAPPING, SAFILO_SAP_STYLE_DATE_ATTRS, SAFILO_SAP_STYLE_FLAG_ATTRS, sTableName);
								}
								if (SAFILO_SAP_UPDATES_DISTR_SET.equals(sSetName)) {
									if (_debug) {
										jpoLog(mxLog, "Invoking sendDistrMktToSAP (STYLE) for bus " + sStyleName + " (" + sBusId + ")");
									}
									sTableName = "ES_DISTR_MARKET";
									iRetCode = sendDistrMktToSAP(context, sBusId, false, true);
								}
								if (SAFILO_SAP_UPDATES_DESCR_SET.equals(sSetName)) {
									if (_debug) {
										jpoLog(mxLog, "Invoking sendExtDescrToSAP (STYLE) for bus " + sStyleName + " (" + sBusId + ")");
									}
									sTableName = "ES_EXTENDED_DESCR";
									iRetCode = sendExtDescrToSAP(context, sBusId, true);
								}
							}
						}
					} else if ("TRUE".equalsIgnoreCase(isKindOfSku)) {
						String sSkuName = (String) mapBus.get(SELECT_NAME);
						String sSkuCurrent = (String) mapBus.get(SELECT_CURRENT);
						if (STATE_ACTIVE.equals(sSkuCurrent) || STATE_CANCELLED.equalsIgnoreCase(sSkuCurrent)) {
							if (SAFILO_SAP_UPDATES_SET.equals(sSetName)) {
								if (_debug) {
									jpoLog(mxLog, "Invoking sendToSAP (SKU) for bus " + sSkuName + " (" + sBusId + ")");
								}
								getAndSendLensTechForSku(context, sBusId);
								sTableName = "ES_SKU";
								iRetCode = sendToSAP(context, sBusId, SAFILO_SAP_MANDATORY_SKU_ATTRS, SAFILO_SAP_COUNTER_FOR_SKU, SAFILO_SAP_SKU_ATTRS_MAPPING, SAFILO_SAP_SKU_DATE_ATTRS, SAFILO_SAP_SKU_FLAG_ATTRS, sTableName);
								if (iRetCode == 0) {
									// iRetCode = sendCommColorToSAP(context, sBusId, false);
								}
							}
							if (SAFILO_SAP_UPDATES_DISTR_SET.equals(sSetName)) {
								if (_debug) {
									jpoLog(mxLog, "Invoking sendDistrMktToSAP (SKU) for bus " + sSkuName + " (" + sBusId + ")");
								}
								sTableName = "ES_DISTR_MARKET";
								iRetCode = sendDistrMktToSAP(context, sBusId, false, true);
							}
							if (SAFILO_SAP_UPDATES_DESCR_SET.equals(sSetName)) {
								if (_debug) {
									jpoLog(mxLog, "Invoking sendExtDescrToSAP (SKU) for bus " + sSkuName + " (" + sBusId + ")");
								}
								sTableName = "ES_EXTENDED_DESCR";
								iRetCode = sendExtDescrToSAP(context, sBusId, true);
							}
						} else {
							if (_debug) {
								jpoLog(mxLog, "Skipping sendToSAP (SKU) for bus " + sSkuName + " (" + sBusId + "): current=" + sSkuCurrent);
							}
						}
					} else if (TYPE_SAFILO_COLORTECHNICAL.equals(sBusType)) {
						String sBusCurrent = (String) mapBus.get(SELECT_CURRENT);
						if (!"Inspiration".equalsIgnoreCase(sBusCurrent)) {
							iRetCode = sendToSAP(context, sBusId, SAFILO_SAP_MANDATORY_COLOR_TECH_ATTRS, SAFILO_SAP_COUNTER_FOR_COLOR_TECH, SAFILO_SAP_COLOR_TECH_ATTRS_MAPPING, null, null, "ES_COLOR_TECH");
							;
						}
					} else if (TYPE_SAFILO_LENSCOLOR_TECHNICAL.equals(sBusType)) {
						String sBusCurrent = (String) mapBus.get(SELECT_CURRENT);
						if (!"Inspiration".equalsIgnoreCase(sBusCurrent)) {
							iRetCode = sendToSAP(context, sBusId, SAFILO_SAP_MANDATORY_LENSTECH_ATTRS, SAFILO_SAP_COUNTER_FOR_LENS_TECH, SAFILO_SAP_LENSTECH_ATTRS_MAPPING, SAFILO_SAP_LENSTECH_DATE_ATTRS, SAFILO_SAP_LENSTECH_FLAG_ATTRS, "ES_LENTECH");
						}
					}
					//          else if ( "TRUE".equalsIgnoreCase(isKindOfLensTech) )
					//          {
					//            jpoLog(mxLog, "Invoking sendToSAP (SKU) for bus " + sBusId);
					//            iRetCode = sendToSAP(context, sBusId, SAFILO_SAP_MANDATORY_LENSTECH_ATTRS, SAFILO_SAP_COUNTER_FOR_LENS_TECH, SAFILO_SAP_LENSTECH_ATTRS_MAPPING, SAFILO_SAP_LENSTECH_DATE_ATTRS, SAFILO_SAP_LENSTECH_FLAG_ATTRS, "ES_LENTECH");
					//          }
					else {
						if (_debug) {
							jpoLog(mxLog, "Unable to invoke sendToSAP for bus " + sBusId);
						}
					}
					slUpdated.add(sBusId);
				} catch (Exception e) {
					e.printStackTrace();
					String sExcDetails = getExceptionDetails(e);
					slUpdated.add(sBusId);
					String sDate = getDateForSAP();
					String sErrMsg = "BUS <" + sBusId + ">: " + sExcDetails;
					createErrorDetail(context, mxLog, "NA", sTableName, sDate, SAFILO_SAP_UPDATE_ERROR_110, sErrMsg, "E");
					if (_debug) {
						mxLog.close();
					}
				}
			}
			// remove the processed object from the set
			if (slUpdated != null && !slUpdated.isEmpty() && slUpdated.size() > 0) {
				SetUtil.removeMembers(context, sSetName, slUpdated, false);
			}
		}
		if (_debug) {
			mxLog.close();
		}
		return iRetCode;
	}


	private String updateSapStatus(
			Context context,
			BufferedWriter mxLog) {
		// initialize the return variable
		String sReturn = new String("OK");
		//String sResult = new String("");
		boolean isError = false;

		// look for new part objects
		MapList mlToUpdate = new MapList();

		// retrieve the required information
		StringList sSelects = new StringList(1);
		sSelects.add(SELECT_ID);
		// the following select are no longer needed
		//sSelects.add(SELECT_CURRENT);
		//sSelects.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR);
		//sSelects.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_SKU_CODE);
		//sSelects.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MMSTA);
		//sSelects.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_EAN11);

		// original where clause:
		//String sTxtWhere = new String("(current == " + STATE_INSERTED + ")&&(" + SELECT_ATTRIBUTE_SAFILO_ENOSAP_STATUS + " == " + SAFILO_SAP_OK_000 + ")");
		try {
			//mlToUpdate = DomainObject.findObjects(context, TYPE_SAFILO_ENOSAP_SE_MD_SKU_STATUS, "*", "*", "*", VAULT_ENOSAP, sTxtWhere, false, sSelects);
			mlToUpdate = getFromTableSeMdSkuStatus(context);
			if (_debug) {
				jpoLog(mxLog, "updateFromSAP: found <" + mlToUpdate.size() + "> SE_MD_SKU_STATUS objects to process");
			}
		} catch (Exception e) {
			e.printStackTrace();
			isError = true;
		}
		if (!isError) {
			if ((mlToUpdate != null) && (mlToUpdate.size() > 0)) {
				Iterator itrToUpdate = mlToUpdate.iterator();

				while (itrToUpdate.hasNext()) {

					// retrieve the passed information
					Map mObj = (Map) itrToUpdate.next();
					//String sEnoSapBusCurr            = (String)mObj.get(SELECT_CURRENT);
					//if ( !STATE_INSERTED.equals(sEnoSapBusCurr) )
					//{
					//  continue;
					//}
					String sEnoSapBusID = (String) mObj.get(SELECT_ID);
					String sModelCode = (String) mObj.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR);
					String sSkuCode = (String) mObj.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_SKU_CODE);

					if (sSkuCode == null || "".equals(sSkuCode.trim()) || sSkuCode.length() <= 0) {
						// update the model object
						MapList mlEyewear = new MapList();
						try {
							mlEyewear = DomainObject.findObjects(context, TYPE_EYEWEAR, sModelCode, "*", "*", "*", "revision == last", true, sSelects);
						} catch (FrameworkException e) {
							e.printStackTrace();
							String sExcDet = getExceptionDetails(e);
							if (_debug) {
								jpoLog(mxLog, "upateSapStatus - findObjects (Eyewear " + sModelCode + ") error:\n" + sExcDet);
							}
							isError = true;
						}
						if (!isError) {
							if ((mlEyewear != null) && (mlEyewear.size() == 1)) {
								Map mModel = (Map) mlEyewear.get(0);
								String sModelId = (String) mModel.get(SELECT_ID);
								boolean isTrans = false;
								try {
									String sCrossPlantStatus = (String) mObj.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MMSTA);

									// start transaction
									MqlUtil.mqlCommand(context, "start transaction", new String[]{});
									isTrans = true;

									DomainObject doModel = DomainObject.newInstance(context, sModelId);
									doModel.setAttributeValue(context, ATTRIBUTE_SAFILO_CROSSPLANTSTATUS, sCrossPlantStatus);

									// promote the service object
									//mxBus.promote(context, sEnoSapBusID);

									// update the STATUS
									DomainObject doEnoSap = DomainObject.newInstance(context, new BusinessObject(TYPE_SAFILO_ENOSAP_SE_MD_SKU_STATUS, sEnoSapBusID, "", VAULT_ENOSAP));
									doEnoSap.setAttributeValue(context, ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_PROCESSED_100);
									doEnoSap.promote(context);

									MqlUtil.mqlCommand(context, "commit transaction", new String[]{});
								} catch (Exception e) {
									e.printStackTrace();
									if (isTrans) {
										try {
											MqlUtil.mqlCommand(context, "abort transaction", new String[]{});
										} catch (FrameworkException e1) {
											e1.printStackTrace();
										}
									}
								}
							} else {
								// no model found
								String sErrMsg = new String("");
								if (mlEyewear != null) {
									sErrMsg = "Found <" + mlEyewear.size() + "> objects with model code=<" + sModelCode + ">";
								} else {
									sErrMsg = "mlEyewear is null, no objects found with model code=<" + sModelCode + ">";
								}
								Calendar cal = Calendar.getInstance();
								Date dayDate = cal.getTime();
								SimpleDateFormat fdata = new SimpleDateFormat("yyyy-MM-dd");
								String sDate = fdata.format(dayDate);
								if (_debug) {
									jpoLog(mxLog, sDate + " sendToSAP (updateSapStatus): " + sErrMsg);
								}

								// promote the service object
								try {
									// 20160415 - MB: SAP is sending info for all the codes, disabling error record generation
									createErrorDetail(context, mxLog, sEnoSapBusID, "SE_MD_SKU_STATUS", sDate, SAFILO_SAP_ERROR_008, sErrMsg, "E");
									//mxBus.promote(context, sEnoSapBusID);

									// update the STATUS
									DomainObject doEnoSap = DomainObject.newInstance(context, new BusinessObject(TYPE_SAFILO_ENOSAP_SE_MD_SKU_STATUS, sEnoSapBusID, "", VAULT_ENOSAP));
									doEnoSap.setAttributeValue(context, ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_ERROR_008);
									doEnoSap.promote(context);
								} catch (Exception e) {
									e.printStackTrace();
									String sExcDet = getExceptionDetails(e);
									if (_debug) {
										jpoLog(mxLog, "upateSapStatus -  (SE_MD_SKU_STATUS " + sEnoSapBusID + " model code=<" + sModelCode + ">) error:\n" + sExcDet);
									}
									isError = true;
								}
							}
						}
					} else {
						// update the sku object
						MapList mlSku = new MapList();
						try {
							mlSku = DomainObject.findObjects(context, TYPE_SAFILO_SKU, sSkuCode, "*", "*", "*", "revision == last", true, sSelects);
						} catch (FrameworkException e) {
							e.printStackTrace();
							isError = true;
							String sExcDet = getExceptionDetails(e);
							if (_debug) {
								jpoLog(mxLog, "upateSapStatus - findObjects (Sku " + sSkuCode + ") error:\n" + sExcDet);
							}
						}
						if (!isError) {
							if ((mlSku != null) && (mlSku.size() == 1)) {
								Map mSku = (Map) mlSku.get(0);
								String sSkuId = (String) mSku.get(SELECT_ID);
								boolean isTrans = false;
								try {
									String sSkuCrossPlantStatus = (String) mObj.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MMSTA);
									String sSkuEan11 = (String) mObj.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_EAN11);
									//
									if (sSkuEan11 == null || "".equals(sSkuEan11.trim()) || sSkuEan11.length() <= 0) {
										sSkuEan11 = "000";
									}

									// start transaction
									MqlUtil.mqlCommand(context, "start transaction", new String[]{});
									isTrans = true;

									DomainObject doModel = DomainObject.newInstance(context, sSkuId);
									doModel.setAttributeValue(context, ATTRIBUTE_SAFILO_SKUCROSSPLANTSTATUS, sSkuCrossPlantStatus);
									doModel.setAttributeValue(context, ATTRIBUTE_SAFILO_UPC_CODE, sSkuEan11);

									// promote the service object
									//mxBus.promote(context, sEnoSapBusID);

									// update the STATUS
									DomainObject doEnoSap = DomainObject.newInstance(context, new BusinessObject(TYPE_SAFILO_ENOSAP_SE_MD_SKU_STATUS, sEnoSapBusID, "", VAULT_ENOSAP));
									doEnoSap.setAttributeValue(context, ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_PROCESSED_100);
									doEnoSap.promote(context);

									MqlUtil.mqlCommand(context, "commit transaction", new String[]{});
								} catch (Exception e) {
									e.printStackTrace();
									if (isTrans) {
										try {
											MqlUtil.mqlCommand(context, "abort transaction", new String[]{});
										} catch (FrameworkException e1) {
											e1.printStackTrace();
										}
									}
								}
							} else {
								// no sku found
								String sErrMsg = new String("");
								if (mlSku != null) {
									sErrMsg = "Found <" + mlSku.size() + "> objects with sku code=<" + sSkuCode + ">";
								} else {
									sErrMsg = "mlSku is null, no objects found with sku code=<" + sSkuCode + ">";
								}
								Calendar cal = Calendar.getInstance();
								Date dayDate = cal.getTime();
								SimpleDateFormat fdata = new SimpleDateFormat("yyyy-MM-dd");
								String sDate = fdata.format(dayDate);
								if (_debug) {
									jpoLog(mxLog, sDate + " sendToSAP (updateSapStatus): " + sErrMsg);
								}

								// promote the service object
								try {
									// 20160415 - MB: SAP is sending info for all the codes, disabling error record generation
									createErrorDetail(context, mxLog, sEnoSapBusID, "SE_MD_SKU_STATUS", sDate, SAFILO_SAP_ERROR_008, sErrMsg, "E");

									// promote the service object
									//mxBus.promote(context, sEnoSapBusID);

									// update the STATUS
									DomainObject doEnoSap = DomainObject.newInstance(context, new BusinessObject(TYPE_SAFILO_ENOSAP_SE_MD_SKU_STATUS, sEnoSapBusID, "", VAULT_ENOSAP));
									doEnoSap.setAttributeValue(context, ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_ERROR_008);
									doEnoSap.promote(context);
								} catch (Exception e) {
									e.printStackTrace();
									String sExcDet = getExceptionDetails(e);
									if (_debug) {
										jpoLog(mxLog, "upateSapStatus -  (SE_MD_SKU_STATUS " + sEnoSapBusID + " sku code=<" + sSkuCode + ">) error:\n" + sExcDet);
									}
									isError = true;
								}
							}
						}
					}
				}
			}
		}

		return sReturn;
	}

	private String updateSkuEan11(
			Context context,
			BufferedWriter mxLog) {
		// initialize the return variable
		String sReturn = new String("OK");
		//String sResult = new String("");
		boolean isError = false;

		// look for new part objects
		MapList mlToUpdate = new MapList();

		// retrieve the required information
		StringList sSelects = new StringList(1);
		sSelects.add(SELECT_ID);
		// the following selects are no longer needed
		//sSelects.add(SELECT_CURRENT);
		//sSelects.add(SELECT_NAME);
		//sSelects.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR);
		//sSelects.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_PLM_SKU_CODE);
		//sSelects.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_ZMSTAE);
		//sSelects.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_EAN11);

		// original where clause:
		// String sTxtWhere = new String("(current == " + STATE_INSERTED + ")&&(" + SELECT_ATTRIBUTE_SAFILO_ENOSAP_STATUS + " == " + SAFILO_SAP_OK_000 + ")");
		try {
			//mlToUpdate = DomainObject.findObjects(context, TYPE_SAFILO_ENOSAP_ES_SKU, "*", "*", "*", VAULT_ENOSAP, sTxtWhere, false, sSelects);
			mlToUpdate = getFromTableEsSku(context);
			if (_debug) {
				jpoLog(mxLog, "updateFromSAP: found <" + mlToUpdate.size() + "> ES_SKU objects to process");
			}
		} catch (Exception e) {
			e.printStackTrace();
			isError = true;
			String sExcDet = getExceptionDetails(e);
			if (_debug) {
				jpoLog(mxLog, "updateSkuEan11 - findObjects (ES_SKU) error:\n" + sExcDet);
			}
		}
		if (!isError) {
			if ((mlToUpdate != null) && (mlToUpdate.size() > 0)) {
				Iterator itrToUpdate = mlToUpdate.iterator();

				while (itrToUpdate.hasNext()) {

					// retrieve the passed information
					Map mObj = (Map) itrToUpdate.next();
					//String sEnoSapBusCurr            = (String)mObj.get(SELECT_CURRENT);
					//if ( !STATE_INSERTED.equals(sEnoSapBusCurr) )
					//{
					//  continue;
					//}
					String sEnoSapBusID = (String) mObj.get(SELECT_ID);
					String sEnoSapBusName = (String) mObj.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_ROW_ID);
					//String sModelCode       = (String)mObj.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR);
					String sSkuCode = (String) mObj.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_PLM_SKU_CODE);

					if (sSkuCode == null || "".equals(sSkuCode.trim()) || sSkuCode.length() <= 0) {
						String sErrMsg = new String("");
						sErrMsg = "sSkuCode is  <" + sSkuCode + "> unable to proceed";
						Calendar cal = Calendar.getInstance();
						Date dayDate = cal.getTime();
						SimpleDateFormat fdata = new SimpleDateFormat("yyyy-MM-dd");
						String sDate = fdata.format(dayDate);
						if (_debug) {
							jpoLog(mxLog, sDate + " sendToSAP (updateSkuEan11): " + sErrMsg);
						}
						createErrorDetail(context, mxLog, sEnoSapBusName, "ES_SKU", sDate, SAFILO_SAP_ERROR_008, sErrMsg, "E");
					} else {
						// update the sku object
						MapList mlSku = new MapList();
						try {
							mlSku = DomainObject.findObjects(context, TYPE_SAFILO_SKU, sSkuCode, "*", "*", "*", "revision == last", true, sSelects);
						} catch (FrameworkException e) {
							e.printStackTrace();
							isError = true;
						}
						if (!isError) {
							if ((mlSku != null) && (mlSku.size() == 1)) {
								Map mSku = (Map) mlSku.get(0);
								String sSkuId = (String) mSku.get(SELECT_ID);
								boolean isTrans = false;
								try {
									//String sSkuCrossPlantStatus = (String)mObj.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MMSTA);
									String sSkuEan11 = (String) mObj.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_EAN11);
									Calendar cal = Calendar.getInstance();
									Date dayDate = cal.getTime();
									SimpleDateFormat fdata = new SimpleDateFormat("yyyy-MM-dd");
									String sDate = fdata.format(dayDate);
									if (_debug) {
										jpoLog(mxLog, sDate + " sendToSAP (updateSkuEan11 for SKU " + sSkuCode + "): sSkuEan11=<" + sSkuEan11 + ">");
									}

									// start transaction
									MqlUtil.mqlCommand(context, "start transaction", new String[]{});
									MqlUtil.mqlCommand(context, "trigger off", new String[]{});
									isTrans = true;

									DomainObject doSku = DomainObject.newInstance(context, sSkuId);
									//doModel.setAttributeValue(context, ATTRIBUTE_SAFILO_SKUCROSSPLANTSTATUS, sSkuCrossPlantStatus);
									if (sSkuEan11 == null || "".equals(sSkuEan11.trim()) || sSkuEan11.length() <= 0) {
										sSkuEan11 = "000";
									}
									doSku.setAttributeValue(context, ATTRIBUTE_SAFILO_UPC_CODE, sSkuEan11);

									// send the distribution market data
									sendDistrMktToSAP(context, sSkuId, true, false);

									// send the extended description
									sendExtDescrToSAP(context, sSkuId, false);

									// promote the service object
									//mxBus.promote(context, sEnoSapBusID);

									// update the STATUS
									DomainObject doEnoSap = DomainObject.newInstance(context, new BusinessObject(TYPE_SAFILO_ENOSAP_ES_SKU, sEnoSapBusID, "", VAULT_ENOSAP));
									doEnoSap.setAttributeValue(context, ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_PROCESSED_100);
									doEnoSap.promote(context);

									MqlUtil.mqlCommand(context, "trigger on", new String[]{});
									MqlUtil.mqlCommand(context, "commit transaction", new String[]{});
								} catch (Exception e) {
									e.printStackTrace();
									if (isTrans) {
										try {
											MqlUtil.mqlCommand(context, "abort transaction", new String[]{});
										} catch (FrameworkException e1) {
											e1.printStackTrace();
										}
									}
								}
							} else {
								// no sku found
								String sErrMsg = new String("");
								sErrMsg = "Found <" + mlSku.size() + "> objects with sku code=<" + sSkuCode + ">";
								Calendar cal = Calendar.getInstance();
								Date dayDate = cal.getTime();
								SimpleDateFormat fdata = new SimpleDateFormat("yyyy-MM-dd");
								String sDate = fdata.format(dayDate);
								if (_debug) {
									jpoLog(mxLog, sDate + " sendToSAP (updateSkuEan11): " + sErrMsg);
								}
								createErrorDetail(context, mxLog, sEnoSapBusName, "ES_SKU", sDate, SAFILO_SAP_ERROR_008, sErrMsg, "E");
								// promote the service object
								try {
									//mxBus.promote(context, sEnoSapBusID);

									// update the STATUS
									DomainObject doEnoSap = DomainObject.newInstance(context, new BusinessObject(TYPE_SAFILO_ENOSAP_ES_SKU, sEnoSapBusID, "", VAULT_ENOSAP));
									doEnoSap.setAttributeValue(context, ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_ERROR_008);
									doEnoSap.promote(context);
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						}
					}
				}
			}
		}

		return sReturn;
	}

	private String renamePrototypes(
			Context context,
			BufferedWriter mxLog) {
		// initialize the return variable
		String sReturn = new String("OK");
		//String sResult = new String("");
		boolean isError = false;

		// look for new part objects
		MapList mlNewCodes = new MapList();

		// retrieve the required information
		StringList sSelects = new StringList(1);
		sSelects.add(SELECT_ID);
		// the following selects are no longer needed
		//sSelects.add(SELECT_CURRENT);
		//sSelects.add(SELECT_NAME);
		//sSelects.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_BISMT);
		//sSelects.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR);
		//sSelects.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE);

		// original where clause:
		// String sTxtWhere = new String("(current == " + STATE_INSERTED + ")&&((" + SELECT_ATTRIBUTE_SAFILO_ENOSAP_STATUS + " == " + SAFILO_SAP_OK_000 + ")||(" + SELECT_ATTRIBUTE_SAFILO_ENOSAP_STATUS + " == " + SAFILO_SAP_WARNING_004 + "))");
		try {
			//mlNewCodes = DomainObject.findObjects(context, TYPE_SAFILO_ENOSAP_ES_STYLE, "*", "*", "*", VAULT_ENOSAP, sTxtWhere, false, sSelects);
			mlNewCodes = getFromTableEsStyle(context);
			if (_debug) {
				jpoLog(mxLog, "updateFromSAP: found <" + mlNewCodes.size() + "> ES_STYLE objects to process");
			}
		} catch (Exception e) {
			e.printStackTrace();
			isError = true;
		}
		if (!isError) {
			if ((mlNewCodes != null) && (mlNewCodes.size() > 0)) {
				Iterator itrNewCodes = mlNewCodes.iterator();

				while (itrNewCodes.hasNext()) {

					// retrieve the passed information
					Map mObj = (Map) itrNewCodes.next();
					//String sEnoSapBusCurr            = (String)mObj.get(SELECT_CURRENT);
					//if ( !STATE_INSERTED.equals(sEnoSapBusCurr) )
					//{
					//  continue;
					//}
					String sEnoSapBusID = (String) mObj.get(SELECT_ID);
					String sPrototypeCode = (String) mObj.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_BISMT);
					String sModelCode = (String) mObj.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR);
					String sUpdateType = (String) mObj.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE);
					String sEnoSapStatus = (String) mObj.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_STATUS);
					if (_debug) {
						jpoLog(mxLog, "updateFromSAP:\n sPrototypeCode <" + sPrototypeCode + "> \n sModelCode <" + sModelCode + "> \n sUpdateType <" + sUpdateType + ">");
					}

					if ("U".equals(sUpdateType.trim())) {
						// promote the service object
						try {
							if (_debug) {
								jpoLog(mxLog, "PROMOTING sPrototypeCode <" + sPrototypeCode + "> \n sModelCode <" + sModelCode + "> \n sUpdateType <" + sUpdateType + ">");
							}
							//mxBus.promote(context, sEnoSapBusID);

							// update the STATUS
							DomainObject doEnoSap = DomainObject.newInstance(context, new BusinessObject(TYPE_SAFILO_ENOSAP_ES_STYLE, sEnoSapBusID, "", VAULT_ENOSAP));
							if (SAFILO_SAP_WARNING_004.equals(sEnoSapStatus)) {
								doEnoSap.setAttributeValue(context, ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_WARNING_104);
							} else {
								doEnoSap.setAttributeValue(context, ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_PROCESSED_100);
							}
							doEnoSap.promote(context);
							if (_debug) {
								jpoLog(mxLog, "PROMOTED  sPrototypeCode <" + sPrototypeCode + "> \n sModelCode <" + sModelCode + "> \n sUpdateType <" + sUpdateType + ">");
							}
						} catch (Exception e) {
							String sExc = getExceptionDetails(e);
							jpoLog(mxLog, "FAILED:\n" + sExc);
							e.printStackTrace();
						}
					} else if ("".equals(sModelCode.trim())) {
						String sEnoSapBusName = (String) mObj.get(SELECT_NAME);
						String sErrMsg = new String("");
						sErrMsg = "Model Code is NULL for ROW_ID <" + sEnoSapBusName + "> (BISMT=<" + sPrototypeCode + ">)";
						Calendar cal = Calendar.getInstance();
						Date dayDate = cal.getTime();
						SimpleDateFormat fdata = new SimpleDateFormat("yyyy-MM-dd");
						String sDate = fdata.format(dayDate);
						if (_debug) {
							jpoLog(mxLog, sDate + " sendToSAP (renamePrototype): " + sErrMsg);
						}
						createErrorDetail(context, mxLog, sEnoSapBusName, "ES_STYLE", sDate, SAFILO_SAP_ERROR_008, sErrMsg, "E");
						// promote the service object
						try {
							//mxBus.promote(context, sEnoSapBusID);

							// update the STATUS
							DomainObject doEnoSap = DomainObject.newInstance(context, new BusinessObject(TYPE_SAFILO_ENOSAP_ES_STYLE, sEnoSapBusID, "", VAULT_ENOSAP));
							doEnoSap.setAttributeValue(context, ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_ERROR_008);
							doEnoSap.promote(context);
						} catch (Exception e) {
							e.printStackTrace();
						}
					} else {
						// rename the object
						MapList mlEyewear = new MapList();
						try {
							mlEyewear = DomainObject.findObjects(context, TYPE_EYEWEAR, sPrototypeCode, "*", "*", "*", "revision == last", true, sSelects);
						} catch (FrameworkException e) {
							e.printStackTrace();
							isError = true;
						}
						if (!isError) {
							if ((mlEyewear != null) && (mlEyewear.size() == 1)) {
								Map mProto = (Map) mlEyewear.get(0);
								String sProtoId = (String) mProto.get(SELECT_ID);
								boolean isTrans = false;
								try {
									// start transaction
									MqlUtil.mqlCommand(context, "start transaction", new String[]{});
									isTrans = true;

									DomainObject doProto = DomainObject.newInstance(context, sProtoId);
									doProto.setName(context, sModelCode);
									// riaccesi i trigger: servono a Peppe
									//GC commented next line (trigger on/of) for [Ticket#2017100387000406]
									//MqlUtil.mqlCommand(context, "trigger off", new String[] { });
									doProto.setAttributeValue(context, ATTRIBUTE_SAFILO_MODELCODE, sModelCode);
									//MqlUtil.mqlCommand(context, "trigger on", new String[] { });

									// send the distribution market data
									sendDistrMktToSAP(context, sProtoId, true, false);

									// send the extended description
									sendExtDescrToSAP(context, sProtoId, false);

									// check if there are SKU
									checkAndRenameSku(context, doProto, sPrototypeCode, sModelCode, mxLog);

									// promote the service object
									//mxBus.promote(context, sEnoSapBusID);

									// update the STATUS
									DomainObject doEnoSap = DomainObject.newInstance(context, new BusinessObject(TYPE_SAFILO_ENOSAP_ES_STYLE, sEnoSapBusID, "", VAULT_ENOSAP));
									if (SAFILO_SAP_WARNING_004.equals(sEnoSapStatus)) {
										doEnoSap.setAttributeValue(context, ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_WARNING_104);
									} else {
										doEnoSap.setAttributeValue(context, ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_PROCESSED_100);
									}
									doEnoSap.promote(context);

									MqlUtil.mqlCommand(context, "commit transaction", new String[]{});
								} catch (Exception e) {
									e.printStackTrace();
									if (isTrans) {
										try {
											MqlUtil.mqlCommand(context, "abort transaction", new String[]{});
										} catch (FrameworkException e1) {
											e1.printStackTrace();
										}
									}
								}
							} else {
								// no prototype found
								String sErrMsg = new String("");
								if (mlEyewear != null) {
									sErrMsg = "Found <" + mlEyewear.size() + "> objects with prototype code=<" + sPrototypeCode + ">";
								} else {
									sErrMsg = "mlEyewear is null, no objects found with prototype code=<" + sPrototypeCode + ">";
								}
								Calendar cal = Calendar.getInstance();
								Date dayDate = cal.getTime();
								SimpleDateFormat fdata = new SimpleDateFormat("yyyy-MM-dd");
								String sDate = fdata.format(dayDate);
								if (_debug) {
									jpoLog(mxLog, sDate + " sendToSAP (renamePrototype): " + sErrMsg);
								}

								// promote the service object
								try {
									createErrorDetail(context, mxLog, "NA", "ES_STYLE", sDate, SAFILO_SAP_ERROR_008, sErrMsg, "E");
									//mxBus.promote(context, sEnoSapBusID);

									// update the STATUS
									DomainObject doEnoSap = DomainObject.newInstance(context, new BusinessObject(TYPE_SAFILO_ENOSAP_ES_STYLE, sEnoSapBusID, "", VAULT_ENOSAP));
									doEnoSap.setAttributeValue(context, ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_ERROR_008);
									doEnoSap.promote(context);
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						}
					}
				}
			}
		}

		return sReturn;
	}


	private void checkAndRenameSku(
			Context context,
			DomainObject doProto,
			String sPrototypeCode,
			String sModelCode, BufferedWriter mxLog)
			throws MatrixException {
		// navigate the model
		StringList slBusSel = new StringList(1);
		slBusSel.add(SELECT_ID);
		try {
			MapList mlRelatedSku = doProto.getRelatedObjects(context, RELATIONSHIP_SAFILO_PRODUCTS_SKU, TYPE_SAFILO_SKU, slBusSel, null, true, true, (short) 1, "", "", 0);
			Iterator itrSku = mlRelatedSku.iterator();

			while (itrSku.hasNext()) {
				Map mapStr = (Map) itrSku.next();
				String sSkuId = (String) mapStr.get(SELECT_ID);
				renameSku(context, mxLog, sSkuId, sModelCode, sPrototypeCode);
			}
		} catch (Exception e) {
			// get Exception details
			e.printStackTrace();
			String sExcDetails = getExceptionDetails(e);
			throw new MatrixException("Error while Checking/renaming SKU of model " + sModelCode + " (" + sPrototypeCode + "):\n" + sExcDetails);
		}

	}


	private void renameSku(
			Context context,
			BufferedWriter mxLog,
			String sSkuId,
			String sModelCode,
			String sPrototypeCode)
			throws MatrixException {
		// rename the SKU
		String sOldCode = new String("NA");
		try {
			DomainObject doSku = DomainObject.newInstance(context, sSkuId);
			sOldCode = (String) doSku.getInfo(context, SELECT_NAME);
			String sNewCode = sModelCode + sOldCode.substring(sPrototypeCode.length());
			doSku.setName(context, sNewCode);
			doSku.setAttributeValue(context, ATTRIBUTE_SAFILO_MODELCODE, sModelCode);
			doSku.setAttributeValue(context, ATTRIBUTE_SAFILO_SKUCODE, sNewCode);
		} catch (FrameworkException e) {
			// get exception details
			e.printStackTrace();
			String sExcDetails = getExceptionDetails(e);
			throw new MatrixException("Error while renaming SKU " + sOldCode + ":\n" + sExcDetails);
		}

	}

	//Start GC 20 07 2017 - sending to SAP without transaction
	public int sendToSAP(Context context, String sObjectId, String sMandatoryAttrMapProgram, String sCountName, String sAttrsMappingMap, String sDateAttrsMap, String sFlagAttrsMap, String sTableName) throws Exception {
		return sendToSAP(context, sObjectId, sMandatoryAttrMapProgram, sCountName, sAttrsMappingMap, sDateAttrsMap, sFlagAttrsMap, sTableName, true);
	}

	public int sendToSAPnoTransaction(Context context, String sObjectId, String sMandatoryAttrMapProgram, String sCountName, String sAttrsMappingMap, String sDateAttrsMap, String sFlagAttrsMap, String sTableName) throws Exception {
		return sendToSAP(context, sObjectId, sMandatoryAttrMapProgram, sCountName, sAttrsMappingMap, sDateAttrsMap, sFlagAttrsMap, sTableName, false);
	}
	//End GC 20 07 2017

	/**
	 * @param context
	 * @param args
	 * @return
	 * @throws MatrixException
	 */
	public int sendToSAP(Context context, String sObjectId, String sMandatoryAttrMapProgram, String sCountName, String sAttrsMappingMap, String sDateAttrsMap, String sFlagAttrsMap, String sTableName, boolean isTrans)
			throws Exception {

		int iRetCode = 0;
		String sId = new String("");
		String sDate = new String("");
		//String sJobId          = new String("");
		BufferedWriter mxLog = null;
		//boolean isTrans        = false;

		try {
			if (_debug) {
				MatrixLogWriter mtxw = new MatrixLogWriter(context, "TOSAP.log", "LOGWRITER", false);
				mxLog = new BufferedWriter(mtxw);
			}

			emxContextUtil_mxJPO ShadowAgent = new emxContextUtil_mxJPO(context, null);
			ShadowAgent.pushContext(context, null);

			// get the time stamp
			String sTimeStamp = getTimestamp();

			sDate = getDateForSAP();
			if (_debug) {
				jpoLog(mxLog, sDate + " sendToSAP: started on bus " + sObjectId + " with timestamp " + sTimeStamp);
			}

			// check mandatory attributes
			String sMissingAttr = checkMandatoryAttributesForSAP(context, sObjectId, sMandatoryAttrMapProgram, mxLog);
			if ("".equals(sMissingAttr)) {
				// retrieve object information
				// get the attributes to check
				//StringList slAttrs     = (StringList) getStringListFromMapProgram(context, sAttrsMappingMap);
				StringList slDateAttrs = new StringList();
				if (sDateAttrsMap != null) {
					slDateAttrs = (StringList) getStringListFromMapProgram(context, sDateAttrsMap);
				} else {
					slDateAttrs.add("DUMMY");
				}
				StringList slFlagAttrs = new StringList();
				if (sFlagAttrsMap != null) {
					slFlagAttrs = (StringList) getStringListFromMapProgram(context, sFlagAttrsMap);
				} else {
					slFlagAttrs.add("DUMMY");
				}

				HashMap hmAttrsMap = (HashMap) getHashMapFromMapProgram(context, sAttrsMappingMap);
				HashMap hmValLength = (HashMap) getHashMapFromMapProgram(context, SAFILO_SAP_ATTR_LENGTH_MAP);
				//jpoLog(mxLog, hmValLength.toString());

				//slAttrs = (StringList) hmAttrsMap.keySet();
				//jpoLog(mxLog, "slAttrs2 = " + slAttrs);

				Set setKeyAttr = (Set) hmAttrsMap.keySet();
				Iterator isetKeyList = setKeyAttr.iterator();
				StringList slAttrs = new StringList(setKeyAttr.size());

				for (int i = 0; i < setKeyAttr.size(); i++) {
					slAttrs.add((String) isetKeyList.next());
				}
				//jpoLog(mxLog, "\n slAttrs = " + slAttrs);

				// get the attributes values
				//DomainObject domObject = new DomainObject(sObjectId);
				DomainObject domObject = DomainObject.newInstance(context, sObjectId);
				domObject.open(context);
				Map mAttrsVal = domObject.getInfo(context, slAttrs);
				//jpoLog(mxLog, "\n????\n mAttrsVal = " + mAttrsVal + "\n????\n");

				// define the hash map of the attributes
				HashMap hmAdapletAttr = new HashMap();
				for (int i = 0; i < slAttrs.size(); i++) {
					String sAttrName = (String) slAttrs.get(i);
					String sAttrVal = (String) mAttrsVal.get(sAttrName);
					if (sAttrVal == null) {
						continue;
					}
					if ("".equals(sAttrVal)) {
						continue;
					}
					if ("-".equals(sAttrVal)) {
						sAttrVal = "";
					}
					if (slDateAttrs.contains(sAttrName)) {
						sAttrVal = getDateField(sAttrVal, mxLog);
					} else if (slFlagAttrs.contains(sAttrName)) {
						if ("Y".equalsIgnoreCase(sAttrVal) || "1".equals(sAttrVal) || "TRUE".equalsIgnoreCase(sAttrVal)) {
							sAttrVal = "X";
						} else {
							sAttrVal = "";
						}
					} else if (sAttrVal.indexOf('-') > 0) {
						// 2017.07.31 - MB: excluded the SAFILO_SUPPLIER_CODE attribute
						if (!"attribute[Marketing Name]".equals(sAttrName)
								&& !SELECT_ATTRIBUTE_SAFILO_RELEASE.equals(sAttrName)
								&& !SELECT_ATTRIBUTE_SAFILO_SUPPLIER_CODE.equals(sAttrName)
								&& !"to[Planned Product].businessobject.name".equals(sAttrName)
								&& !"to[SAFILO_Products_SKU].businessobject.attribute[SAFILO_Release]".equals(sAttrName)
								&& !"to[SAFILO_Products_SKU].businessobject.to[Planned Product].businessobject.name".equals(sAttrName)) {
							StringList slTemp = FrameworkUtil.split(sAttrVal, "-");
							String sCodeVal = (String) slTemp.get(0);
							sAttrVal = sCodeVal.trim();
						} else {
							if (SELECT_ATTRIBUTE_SAFILO_RELEASE.equals(sAttrName) || "to[SAFILO_Products_SKU].businessobject.attribute[SAFILO_Release]".equals(sAttrName)) {
								sAttrVal = getReleaseDate(sAttrVal);
							}
						}
					}
					if ("0".equals(sAttrVal) && "to[Design Responsibility].businessobject.attribute[Organization ID]".equals(sAttrName)) {
						sAttrVal = "";
					}
					// check attribute length
					if (hmValLength.containsKey(hmAttrsMap.get(sAttrName))) {
						int iMaxValLength = Integer.parseInt((String) hmValLength.get(hmAttrsMap.get(sAttrName)));
						//jpoLog(mxLog, "Checking val length for " + (String)hmAttrsMap.get(sAttrName) + " MAX=" + iMaxValLength + " actual " + sAttrVal.length() + "(" + sAttrVal + ")" );
						if (sAttrVal.length() > iMaxValLength) {
							String sWarnMsg = new String("ATTR. <" + sAttrName + "> TRUNCATED from <" + sAttrVal + "> (" + sAttrVal.length() + ") to <" + sAttrVal.substring(0, iMaxValLength) + ">");
							if (_debug) {
								jpoLog(mxLog, sWarnMsg);
							}
							//createErrorDetail(context, mxLog, "NA", sTableName, sDate, SAFILO_SAP_WARNING_104, sWarnMsg, "W");
							sAttrVal = sAttrVal.substring(0, iMaxValLength);
						}
					}
					hmAdapletAttr.put(hmAttrsMap.get(sAttrName), sAttrVal.toUpperCase());
					if (SELECT_ATTRIBUTE_SAFILO_CROSSPLANTSTATUS.equals(sAttrName)) {
						if ("ZZ".equals(sAttrVal)) {
							hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_PLM_STYLE_ABORT, sAttrVal);
						}
					}
					if (SELECT_ATTRIBUTE_SAFILO_SKUCROSSPLANTSTATUS.equals(sAttrName)) {
						if ("ZZ".equals(sAttrVal)) {
							hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_PLM_SKU_ABORT, sAttrVal);
						}
					}
				}

				// SKU only: update the 2ND dimension (last 4 digit of the code)
				if ("ES_SKU".equals(sTableName)) {
					String sSkuCode = (String) domObject.getInfo(context, SELECT_NAME);
					String sSecDim = sSkuCode.substring(sSkuCode.length() - SAFILO_SAP_SKU_2ND_DIM_LENGTH);
					//jpoLog(mxLog, "Updating SKU_2ND_DIM with " + sSecDim);
					hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_PLM_SKU_2ND_DIM, sSecDim);
					// check the seasonal type
					String sSkuSeasonType = (String) domObject.getInfo(context, SELECT_SEASONAL_TYPE_FROM_SKU);
					if (sSkuSeasonType == null || "".equals(sSkuSeasonType.trim()) || sSkuSeasonType.length() <= 0) {
						sSkuSeasonType = (String) domObject.getInfo(context, SELECT_DROPPED_SEASONAL_TYPE_FROM_SKU);
						if (sSkuSeasonType == null || "".equals(sSkuSeasonType.trim()) || sSkuSeasonType.length() <= 0) {
							String sErrMsg = "SKU <" + sSkuCode + ">: unable to get the seasonal type of the SKU. Impossible to proceed. Please, contact the helpdesk";
							throw new MatrixException(sErrMsg);
						} else {
							String sErrMsg = "SKU <" + sSkuCode + ">: the related product has been cancelled. Impossible to proceed with the activation.";
							throw new MatrixException(sErrMsg);
						}
					}
					if (sSkuSeasonType.startsWith("SKU")) {
						if (hmAdapletAttr.containsKey(ATTRIBUTE_SAFILO_ENOSAP_PLM_RELEASE)) {
							hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_PLM_RELEASE, "");
						}
					}
					// check the product type
					String sProdType = (String) domObject.getInfo(context, SELECT_MODEL_TYPE_FROM_SKU);
					if (_debug) {
						jpoLog(mxLog, "sProdType = <" + sProdType + ">");
					}
					if (TYPE_SAFILO_SUNGLASS.equals(sProdType)) {
						String sLenTechId = (String) domObject.getInfo(context, SELECT_ATTRIBUTE_SAFILO_TECHNICALCOLOR);
						if (_debug) {
							jpoLog(mxLog, "sLenTechId = <" + sLenTechId + ">");
						}
						if (ProductLineCommon.isNotNull(sLenTechId)) {
							DomainObject doLensTech = DomainObject.newInstance(context, sLenTechId);
							String sLensColorName = (String) doLensTech.getInfo(context, SELECT_NAME);
							String sLensCommColor = (String) doLensTech.getInfo(context, "to[" + RELATIONSHIP_SAFILO_COLOR_COMMERCIAL_TECHNICAL + "].businessobject.name");
							if (_debug) {
								jpoLog(mxLog, "sLensColorName = <" + sLensColorName + ">");
								jpoLog(mxLog, "sLensCommColor = <" + sLensCommColor + ">");
							}
							String sAttrName = "attribute[SAFILO_LensColorTechnical]";
							hmAdapletAttr.remove(hmAttrsMap.get(sAttrName));
							hmAdapletAttr.put(hmAttrsMap.get(sAttrName), sLensColorName);
							sAttrName = "attribute[SAFILO_LensCommercialColor]";
							hmAdapletAttr.remove(hmAttrsMap.get(sAttrName));
							hmAdapletAttr.put(hmAttrsMap.get(sAttrName), sLensCommColor);
						} else {
							hmAdapletAttr.remove(ATTRIBUTE_SAFILO_ENOSAP_ZLENSTECHCODE);
							hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_ZLENSTECHCODE, "");
							hmAdapletAttr.remove(ATTRIBUTE_SAFILO_ENOSAP_ZLENSCODE);
							hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_ZLENSCODE, "");
						}
					} else {
						hmAdapletAttr.remove(ATTRIBUTE_SAFILO_ENOSAP_ZLENSTECHCODE);
						hmAdapletAttr.remove(ATTRIBUTE_SAFILO_ENOSAP_ZLENSCODE);
						if (_debug) {
							jpoLog(mxLog, "Removed ZLENSTECHCODE and ZLENSCODE: " + hmAdapletAttr);
						}
						hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_ZLENSTECHCODE, "");
						hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_ZLENSCODE, "");
					}
					if (_debug) {
						jpoLog(mxLog, "hmAdapletAttr after SKU modifications " + hmAdapletAttr);
					}
				} else if ("ES_STYLE".equals(sTableName)) {
					String sSeasonType = (String) domObject.getInfo(context, SELECT_SEASONAL_TYPE_FROM_STYLE);
					if (sSeasonType == null || "".equals(sSeasonType) || sSeasonType.length() <= 0) {
						sSeasonType = (String) domObject.getInfo(context, SELECT_SEASONAL_TYPE_FROM_DROPPED_STYLE);
					}
					if (sSeasonType == null || "".equals(sSeasonType) || sSeasonType.length() <= 0) {
						hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_PLM_RELEASE, "");
					} else {
						if (sSeasonType.startsWith("SKU")) {
							if (hmAdapletAttr.containsKey(ATTRIBUTE_SAFILO_ENOSAP_PLM_RELEASE)) {
								hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_PLM_RELEASE, "");
							}
						}
					}
				} else if ("ES_LENTECH".equals(sTableName) || "ES_COLOR_TECH".equals(sTableName)) {
					String sPlmStatus = "AA";
					String sObjCurrent = (String) domObject.getInfo(context, SELECT_CURRENT);
					if ("Adopted".equals(sObjCurrent)) {
						sPlmStatus = "IL";
					} else if ("Archive".equals(sObjCurrent)) {
						sPlmStatus = "ZZ";
					}
					hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_PLM_STATUS, sPlmStatus);
				}
				// add the control attributes
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_JOB_USER, context.getUser());

				if (isCurrentDateAfterReleaseDate(context,domObject)) {
					hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_IGNORE_101);
				} else {
					hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_READY_002);
				}
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_STATUS_DATE, sDate);
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_MSG_TABLE, sTableName);
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_TIMESTAMP, sTimeStamp);
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_SENDER_SYSTEM, "ENO");
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_RECEIVER_SYSTEM, "SAP");
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE, "C");

				if ("ES_STYLE".equals(sTableName)) {
					String sStyleName = (String) domObject.getInfo(context, SELECT_NAME);
					String sStyleCode = (String) domObject.getInfo(context, SELECT_ATTRIBUTE_SAFILO_MODELCODE);
					if (sStyleName.equals(sStyleCode)) {
						hmAdapletAttr.remove(ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE);
						hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE, "U");
					} else {
						hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE, "C");
					}
				}

				if ("ES_SKU".equals(sTableName)) {
					// check the EAN11 attribute, if it is not null then it's an update
					//String sEan11 = (String)hmAdapletAttr.get(ATTRIBUTE_SAFILO_ENOSAP_EAN11);
					String sEan11 = domObject.getInfo(context, SELECT_ATTRIBUTE_SAFILO_UPC_CODE);
					if ((sEan11 == null) || ("".equals(sEan11.trim()))) {
						hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE, "C");
						if (_debug) {
							jpoLog(mxLog, "\n####\nBus <SAFILO_ENOSAP_" + sTableName + "> <" + sId + "> with sEan11:\n" + sEan11 + "\nUPDATE_TYPE:\nC\n\n");
						}
					} else {
						hmAdapletAttr.remove(ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE);
						hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE, "U");
						if (_debug) {
							jpoLog(mxLog, "\n####\nBus <SAFILO_ENOSAP_" + sTableName + "> <" + sId + "> with sEan11:\n" + sEan11 + "\nUPDATE_TYPE:\nU\n\n");
						}
					}
				}

				// start transaction
				if (isTrans) {
					MqlUtil.mqlCommand(context, "start transaction", new String[]{});
				}
				//isTrans = true;

				// create the adaplet object
				sId = getCode(context, mxLog, sCountName);

//        if ( _debug )
//        {
//          jpoLog(mxLog, "\n####\nCreating bus <SAFILO_ENOSAP_"+sTableName+"> <"+sId+"> with attributes:\n"+hmAdapletAttr+"\n\n");
//        }
				BusinessObject boToSap = mxBus.create(context, "SAFILO_ENOSAP_" + sTableName, sId, "", POLICY_SAFILO_ENOSAP, VAULT_ENOSAP);
				DomainObject doToSap = new DomainObject(boToSap);
				doToSap.open(context);
				doToSap.setAttributeValues(context, hmAdapletAttr);
				doToSap.close(context);

				// no errors: commit the transaction
				//MqlUtil.mqlCommand(context, "commit transaction");
				if (isTrans) {
					MqlUtil.mqlCommand(context, "commit transaction", new String[]{});
				}
				if (_debug) {
					jpoLog(mxLog, "\n####\nCreated bus <SAFILO_ENOSAP_" + sTableName + "> <" + sId + ">\n\n");
				}
			} else {
				String sErrMsg = "Mandatory attributes are not valorized:\n" + sMissingAttr + "\nSend to SAP cancelled.";
				if (_debug) {
					jpoLog(mxLog, sErrMsg);
				}
				//createErrorDetail(context, mxLog, "NA", sTableName, sDate, SAFILO_SAP_NOTREADY_108, sErrMsg, "E");
				iRetCode = 1;
			}
		} catch (Exception e) {
			// log the error
			String sExcDetails = "SAFILO_SAP_SendToSAP execution error for BUS with ID " + sObjectId;
			sExcDetails += "\n(SAFILO_SAP with ID=" + sId + " of table " + sTableName + "):\n";
			sExcDetails += getExceptionDetails(e);
			createErrorDetail(context, mxLog, sId, sTableName, sDate, SAFILO_SAP_NOTREADY_108, sExcDetails, "E");

			// errors: roll-back the transaction (?)
			try {
				//MqlUtil.mqlCommand(context, "abort transaction");
				if (isTrans) {
					MqlUtil.mqlCommand(context, "abort transaction", new String[]{});
				}
				sExcDetails += "\n=====\nSAFILO_SAP_SendToSAP rollback OK";
			} catch (Exception e1) {
				// log the error
				sExcDetails += "\n=====\nSAFILO_SAP_SendToSAP rollback error: " + getExceptionDetails(e1);
			}

			iRetCode = 1;
			if (_debug) {
				jpoLog(mxLog, "sendToSAP s'e' rrroottttt!\n" + sExcDetails);
				mxLog.close();
			}
			throw new MatrixException(getLogDate() + " - " + "sendToSAP: " + sExcDetails);
		}
		if (_debug) {
			mxLog.close();
		}
		return iRetCode;
	}


	/**
	 * @param sAttrVal
	 * @return
	 */
	private String getReleaseDate(String sAttrVal) {
		StringList slTemp = FrameworkUtil.split(sAttrVal, "-");
		String sYearVal = (String) slTemp.get(0);
		String sMonthVal = (String) slTemp.get(1);
		sAttrVal = sYearVal + sMonthVal + "01";
		return sAttrVal;
	}


	/**
	 * @return
	 */
	private String getDateForSAP() {
		String sDate;
		Calendar cal = Calendar.getInstance();
		Date dayDate = cal.getTime();
		SimpleDateFormat fdata = new SimpleDateFormat("yyyyMMdd");
		sDate = fdata.format(dayDate);
		return sDate;
	}

	public void createErrorDetail(
			Context context,
			BufferedWriter mxLog,
			String sMsgId,
			String sTableName,
			String sDate,
			String sReturnCode,
			String sError,
			String sErrorType) {
		createErrorDetail(context, mxLog, sMsgId, sTableName, sDate, sReturnCode, sError, sErrorType, true);
	}

	public void createErrorDetail(
			Context context,
			BufferedWriter mxLog,
			String sMsgId,
			String sTableName,
			String sDate,
			String sReturnCode,
			String sError,
			String sErrorType,
			boolean bUseTransaction) {

		// get the time stamp
		String sTimeStamp = getTimestamp(true);

		// set the attributes
		HashMap hmAdapletAttr = new HashMap(8);
		//hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_JOB_USER, context.getUser());
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_MSG_TABLE, sTableName);
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_SENDER_SYSTEM, "ENO");
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_RECEIVER_SYSTEM, "SAP");
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_RETURN_CODE, sReturnCode);
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_MSG_ID, sMsgId);
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_ERROR_MSG, sError);
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_ERROR_TYPE, sErrorType);

		try {
			// start transaction
			if (bUseTransaction) {
				MqlUtil.mqlCommand(context, "start transaction", new String[]{});
			}

			// prepare the sql insert statement
			PreparedStatement addRow = null;

			// sql command template
			String rowToIns = new String("");
//      rowToIns += "Insert into ERROR_DETAIL ([ROW_ID],[MSG_ID],[MSG_TABLE],[TIMESTAMP],[SENDER_SYSTEM]";
//      rowToIns += ",[RECEIVER_SYSTEM],[ERROR_TYPE],[RETURN_CODE],[ERROR_MSG]) ";
//      rowToIns += "values(ROW_ID.nextval,?,?,?,?,?,?,?,?)";
			rowToIns += "Insert into ERROR_DETAIL ([MSG_ID],[MSG_TABLE],[TIMESTAMP],[SENDER_SYSTEM]";
			rowToIns += ",[RECEIVER_SYSTEM],[ERROR_TYPE],[RETURN_CODE],[ERROR_MSG]) ";
			rowToIns += "values(?,?,?,?,?,?,?,?)";

			// prepare the sql command
			Connection enosapConnection = DriverManager.getConnection(connectionUrl);
			// Check for warnings generated during connect
			//jpoLog(mxLog, enosapConnection.getWarnings().toString() );

			// disable autocommit: the transaction will be managed by the program
			if (enosapConnection.getAutoCommit()) {
				enosapConnection.setAutoCommit(false);
			}
			addRow = enosapConnection.prepareStatement(rowToIns);

			// fill the command with the parameters values
			addRow.setString(1, sMsgId);
			addRow.setString(2, sTableName);
			addRow.setString(3, sTimeStamp);
			addRow.setString(4, "ENO");
			addRow.setString(5, "SAP");
			addRow.setString(6, sErrorType);
			addRow.setString(7, sReturnCode);
			addRow.setString(8, sError);

			addRow.executeUpdate();

			// no errors: commit the transaction
			enosapConnection.commit();

			// Check for warnings generated during connect
			//jpoLog(mxLog, enosapConnection.getWarnings().toString() );

// ARRIVEDERCI
			// close explicitly the connection to release db resources
			enosapConnection.close();

			// no errors: commit the transaction
			if (bUseTransaction) {
				MqlUtil.mqlCommand(context, "commit transaction", new String[]{});
			}
			//jpoLog(mxLog, "SAFILO_ENOSAP_UtilsJPO.createErrorDetail - bus created with name = <" + sTimeStamp + ">");
		} catch (Exception e) {
			// Log the error to the file
			String sExcDet = getExceptionDetails(e);
			if (_debug) {
				jpoLog(mxLog, "\n!!! ERROR while creating ERROR_DETAIL with attributes:\n" + hmAdapletAttr + "\nDetails:\n" + sExcDet);
			}
			//e.printStackTrace();
			try {
				if (bUseTransaction) {
					MqlUtil.mqlCommand(context, "abort transaction", new String[]{});
				}
			} catch (FrameworkException e1) {
				String sExcDet1 = getExceptionDetails(e1);
				if (_debug) {
					jpoLog(mxLog, "\n!!! ERROR while aborting transaction.\nDetails:\n" + sExcDet1);
				}
			}
		}
	}


	/**
	 * @param context
	 * @param args
	 * @return
	 * @throws MatrixException
	 */
	public int sendDistrMktToSAP(Context context, String sObjectId, boolean isNew, boolean useTransaction) throws Exception {

		int iRetCode = 0;
		String sId = new String("");
		//String sJobId          = new String("");
		BufferedWriter mxLog = null;
		boolean isTrans = false;
		String sCountName = SAFILO_SAP_COUNTER_FOR_DISTR_MKT;
		String sTableName = "ES_DISTR_MARKET";

		try {
			if (_debug) {
				MatrixLogWriter mtxw = new MatrixLogWriter(context, "TOSAP.log", "LOGWRITER", false);
				mxLog = new BufferedWriter(mtxw);
			}

			emxContextUtil_mxJPO ShadowAgent = new emxContextUtil_mxJPO(context, null);
			ShadowAgent.pushContext(context, null);

			// get the time stamp
			String sTimeStamp = getTimestamp();

			Calendar cal = Calendar.getInstance();
			Date dayDate = cal.getTime();
			SimpleDateFormat fdata = new SimpleDateFormat("yyyyMMdd");
			String sDate = fdata.format(dayDate);
			//jpoLog(mxLog, sDate + " sendToSAP (DistrMkt): started on bus " + sObjectId + " with timestamp " + sTimeStamp);

			// retrieve object information and insert the records in the ENOSAP tables
			// get the attributes to check
			String sAttrsMappingMap = SAFILO_SAP_DISTR_MKT_ATTRS_MAPPING;
			StringList slAttrs = (StringList) getStringListFromMapProgram(context, sAttrsMappingMap);
			//jpoLog(mxLog, "\nslAttrs:" + slAttrs);
			//HashMap    hmAttrsMap  = (HashMap)    getHashMapFromMapProgram(context, sAttrsMappingMap);
			StringList slDateAttrs = (StringList) getStringListFromMapProgram(context, SAFILO_SAP_DISTR_MKT_DATE_ATTRS);
			//StringList slFlagAttrs = (StringList) getStringListFromMapProgram(context, sFlagAttrsMap);
			HashMap hmAttrsMap = (HashMap) getHashMapFromMapProgram(context, sAttrsMappingMap);
			HashMap hmValLength = (HashMap) getHashMapFromMapProgram(context, SAFILO_SAP_ATTR_LENGTH_MAP);

			//jpoLog(mxLog, "\n slAttrs = " + slAttrs);

			// get the attributes values
			DomainObject domObject = new DomainObject(sObjectId);
			Map mAttrsVal = domObject.getInfo(context, slAttrs);
			String sCode = (String) domObject.getInfo(context, SELECT_NAME);

			//jpoLog(mxLog, "\n????\n mAttrsVal = " + mAttrsVal + "\n????\n");

			// define the hash map of the attributes
			HashMap hmAdapletAttr = new HashMap();
			for (int i = 0; i < slAttrs.size(); i++) {
				String sAttrName = (String) slAttrs.get(i);
				String sAttrVal = (String) mAttrsVal.get(sAttrName);
				if (sAttrVal == null) {
					continue;
				}
				if (slDateAttrs.contains(sAttrName)) {
					sAttrVal = getDateField(sAttrVal, mxLog);
				} else if (sAttrVal.indexOf('-') > 0) {
					if (!SELECT_ATTRIBUTE_SAFILO_RELEASE.equals(sAttrName) && !"to[SAFILO_Products_SKU].businessobject.attribute[SAFILO_Release]".equals(sAttrName)) {
						StringList slTemp = FrameworkUtil.split(sAttrVal, "-");
						String sCodeVal = (String) slTemp.get(0);
						sAttrVal = sCodeVal.trim();
					} else {
						sAttrVal = getReleaseDate(sAttrVal);
					}
				}
				if ("".equals(sAttrVal)) {
					continue;
				}
				// check attribute length
				if (hmValLength.containsKey(hmAttrsMap.get(sAttrName))) {
					int iMaxValLength = Integer.parseInt((String) hmValLength.get(hmAttrsMap.get(sAttrName)));
					//jpoLog(mxLog, "Check attr. <" + sAttrName + " (" + hmAttrsMap.get(sAttrName) + ")> length. Value=<" + sAttrVal + "(" + sAttrVal.length() + ") + MAX=" + iMaxValLength);
					if (sAttrVal.length() > iMaxValLength) {
						String sWarnMsg = new String("ATTR. <" + sAttrName + "> TRUNCATED from <" + sAttrVal + "> (" + sAttrVal.length() + ") to <" + sAttrVal.substring(0, iMaxValLength) + ">");
						if (_debug) {
							jpoLog(mxLog, sWarnMsg);
						}
						//createErrorDetail(context, mxLog, "NA", sTableName, sDate, SAFILO_SAP_WARNING_104, sWarnMsg, "W", useTransaction);
						sAttrVal = sAttrVal.substring(0, iMaxValLength);
					}
				}
				hmAdapletAttr.put(hmAttrsMap.get(sAttrName), sAttrVal);
			}

			// SKU only: update the 2ND dimension (last 4 digit of the code)
			String sSkuCode = (String) mAttrsVal.get(SELECT_ATTRIBUTE_SAFILO_SKUCODE);
			if (sSkuCode != null && !"".equals(sSkuCode)) {
				String sSecDim = sSkuCode.substring(sSkuCode.length() - SAFILO_SAP_SKU_2ND_DIM_LENGTH);
				String sSkuCol = domObject.getInfo(context, SELECT_SKU_COLOR_FROM_SKU);
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_SKU_COLOR, sSkuCol);
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_SKU_2ND_DIM, sSecDim);
				if (_debug) {
					jpoLog(mxLog, "Updating SKU_COLOR with " + sSkuCol);
					jpoLog(mxLog, "Updating SKU_2ND_DIM with " + sSecDim);
					//jpoLog(mxLog, "hmAdapletAttr after SKU_2ND_DIM " + hmAdapletAttr);
				}

				// retrieve the PLM_RELEASE, SEASONAL_TYPE and SEASONAL_CODE from the related product
				StringList slForSku = new StringList(3);
				slForSku.add(SELECT_MODEL_RELEASE_DATE_FROM_SKU);
				slForSku.add(SELECT_SEASONAL_CODE_FROM_SKU);
				slForSku.add(SELECT_SEASONAL_TYPE_FROM_SKU);
				Map mForSku = domObject.getInfo(context, slForSku);
				if (_debug) {
					jpoLog(mxLog, "SKU.mForSku <" + mForSku + ">");
				}
				//
				String sModRelDate = (String) mForSku.get(SELECT_MODEL_RELEASE_DATE_FROM_SKU);
				String sSeasonCode = (String) mForSku.get(SELECT_SEASONAL_CODE_FROM_SKU);
				String sSeasonType = (String) mForSku.get(SELECT_SEASONAL_TYPE_FROM_SKU);
				if (sModRelDate != null && !"".equals(sModRelDate) && sModRelDate.length() > 0) {
					hmAdapletAttr.remove(ATTRIBUTE_SAFILO_ENOSAP_PLM_RELEASE);
					hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_PLM_RELEASE, getDateField(sModRelDate, mxLog));
				}
				//if ( sSeasonCode.indexOf('-') > 0 )
				//{
				//  StringList slTemp = FrameworkUtil.split(sSeasonCode, "-");
				//  String sCodeVal = (String)slTemp.get(0);
				//  sSeasonCode = sCodeVal.trim();
				//}
				hmAdapletAttr.remove(ATTRIBUTE_SAFILO_ENOSAP_PLM_SEASONAL_CODE);
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_PLM_SEASONAL_CODE, sSeasonCode);
				if (sSeasonType != null) {
					if (sSeasonType.indexOf('-') > 0) {
						StringList slTemp = FrameworkUtil.split(sSeasonType, "-");
						String sCodeVal = (String) slTemp.get(0);
						sSeasonType = sCodeVal.trim();
					}
				} else {
					sSeasonType = "NOTFOUND";
				}
				hmAdapletAttr.remove(ATTRIBUTE_SAFILO_ENOSAP_PLM_SEASONAL_TYPE);
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_PLM_SEASONAL_TYPE, sSeasonType);
			}
			// add the control attributes
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_JOB_USER, context.getUser());



			if (isCurrentDateAfterReleaseDate(context,domObject)) {
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_IGNORE_101);
			} else {
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_READY_002);
			}

			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_STATUS_DATE, sDate);
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_MSG_TABLE, sTableName);
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_TIMESTAMP, sTimeStamp);
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_SENDER_SYSTEM, "ENO");
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_RECEIVER_SYSTEM, "SAP");
			if (isNew) {
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE, "C");
			} else {
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE, "U");
			}
			//jpoLog(mxLog, "hmAdapletAttr="+hmAdapletAttr);

			// at this point in the hmAdapletAttr we have all the attributes, let's loop on the ref.market
			HashMap hmAdapletAttrNoMarket = hmAdapletAttr;
			if (hmAdapletAttrNoMarket.containsKey(ATTRIBUTE_SAFILO_ENOSAP_ZMARKET)) {
				hmAdapletAttrNoMarket.remove(ATTRIBUTE_SAFILO_ENOSAP_ZMARKET);
			}
			if (hmAdapletAttrNoMarket.containsKey(ATTRIBUTE_SAFILO_ENOSAP_START_SALES)) {
				hmAdapletAttrNoMarket.remove(ATTRIBUTE_SAFILO_ENOSAP_START_SALES);
			}

			if (mAttrsVal.containsKey(SELECT_ATTRIBUTE_SAFILO_REFERENCEMARKET1)) {
				String sAttrMkt1 = (String) mAttrsVal.get(SELECT_ATTRIBUTE_SAFILO_REFERENCEMARKET1);
				if (sAttrMkt1 != null && !"".equals(sAttrMkt1.trim()) && !"-".equals(sAttrMkt1.trim())) {
					StringList slTemp = FrameworkUtil.split(sAttrMkt1, "-");
					String sCodeVal = (String) slTemp.get(0);
					sAttrMkt1 = sCodeVal.trim();
					hmAdapletAttrNoMarket.put(ATTRIBUTE_SAFILO_ENOSAP_ZMARKET, sAttrMkt1);
					String sAttrStart1 = (String) mAttrsVal.get(SELECT_ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET1);
					String sStartSale = getDateField(sAttrStart1, mxLog);
					if (!(null == sStartSale || "".equals(sStartSale) || "NULL".equalsIgnoreCase(sStartSale))) {
						hmAdapletAttrNoMarket.put(ATTRIBUTE_SAFILO_ENOSAP_START_SALES, sStartSale);
						createDistrMarket(context, mxLog, sCountName, sTableName, sTimeStamp, sDate, sCode, hmAdapletAttrNoMarket, useTransaction);
					} else {
						String sErrMsg = new String("ATTR. <" + ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET1 + "> is not valorized <" + sAttrStart1 + ">. Impossible to procede.");
						if (_debug) {
							jpoLog(mxLog, sErrMsg);
						}
						createErrorDetail(context, mxLog, "NA", sTableName, sDate, SAFILO_SAP_ERROR_008, sErrMsg, "E", useTransaction);
					}
				}
			}
			if (mAttrsVal.containsKey(SELECT_ATTRIBUTE_SAFILO_REFERENCEMARKET2)) {
				String sAttrMkt2 = (String) mAttrsVal.get(SELECT_ATTRIBUTE_SAFILO_REFERENCEMARKET2);
				if (sAttrMkt2 != null && !"".equals(sAttrMkt2.trim()) && !"-".equals(sAttrMkt2.trim())) {
					StringList slTemp = FrameworkUtil.split(sAttrMkt2, "-");
					String sCodeVal = (String) slTemp.get(0);
					sAttrMkt2 = sCodeVal.trim();
					hmAdapletAttrNoMarket.put(ATTRIBUTE_SAFILO_ENOSAP_ZMARKET, sAttrMkt2);
					String sAttrStart2 = (String) mAttrsVal.get(SELECT_ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET2);
					String sStartSale = getDateField(sAttrStart2, mxLog);
					if (!(null == sStartSale || "".equals(sStartSale) || "NULL".equalsIgnoreCase(sStartSale))) {
						hmAdapletAttrNoMarket.put(ATTRIBUTE_SAFILO_ENOSAP_START_SALES, sStartSale);
						createDistrMarket(context, mxLog, sCountName, sTableName, sTimeStamp, sDate, sCode, hmAdapletAttrNoMarket, useTransaction);
					} else {
						String sErrMsg = new String("ATTR. <" + ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET2 + "> is not valorized <" + sAttrStart2 + ">. Impossible to procede.");
						if (_debug) {
							jpoLog(mxLog, sErrMsg);
						}
						createErrorDetail(context, mxLog, "NA", sTableName, sDate, SAFILO_SAP_ERROR_008, sErrMsg, "E", useTransaction);
					}
				}
			}
			if (mAttrsVal.containsKey(SELECT_ATTRIBUTE_SAFILO_REFERENCEMARKET3)) {
				String sAttrMkt3 = (String) mAttrsVal.get(SELECT_ATTRIBUTE_SAFILO_REFERENCEMARKET3);
				if (sAttrMkt3 != null && !"".equals(sAttrMkt3.trim()) && !"-".equals(sAttrMkt3.trim())) {
					StringList slTemp = FrameworkUtil.split(sAttrMkt3, "-");
					String sCodeVal = (String) slTemp.get(0);
					sAttrMkt3 = sCodeVal.trim();
					hmAdapletAttrNoMarket.put(ATTRIBUTE_SAFILO_ENOSAP_ZMARKET, sAttrMkt3);
					String sAttrStart3 = (String) mAttrsVal.get(SELECT_ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET3);
					String sStartSale = getDateField(sAttrStart3, mxLog);
					if (!(null == sStartSale || "".equals(sStartSale) || "NULL".equalsIgnoreCase(sStartSale))) {
						hmAdapletAttrNoMarket.put(ATTRIBUTE_SAFILO_ENOSAP_START_SALES, sStartSale);
						createDistrMarket(context, mxLog, sCountName, sTableName, sTimeStamp, sDate, sCode, hmAdapletAttrNoMarket, useTransaction);
					} else {
						String sErrMsg = new String("ATTR. <" + ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET3 + "> is not valorized <" + sAttrStart3 + ">. Impossible to procede.");
						if (_debug) {
							jpoLog(mxLog, sErrMsg);
						}
						createErrorDetail(context, mxLog, "NA", sTableName, sDate, SAFILO_SAP_ERROR_008, sErrMsg, "E", useTransaction);
					}
				}
			}
			if (mAttrsVal.containsKey(SELECT_ATTRIBUTE_SAFILO_REFERENCEMARKET4)) {
				String sAttrMkt4 = (String) mAttrsVal.get(SELECT_ATTRIBUTE_SAFILO_REFERENCEMARKET4);
				if (sAttrMkt4 != null && !"".equals(sAttrMkt4.trim()) && !"-".equals(sAttrMkt4.trim())) {
					StringList slTemp = FrameworkUtil.split(sAttrMkt4, "-");
					String sCodeVal = (String) slTemp.get(0);
					sAttrMkt4 = sCodeVal.trim();
					hmAdapletAttrNoMarket.put(ATTRIBUTE_SAFILO_ENOSAP_ZMARKET, sAttrMkt4);
					String sAttrStart4 = (String) mAttrsVal.get(SELECT_ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET4);
					String sStartSale = getDateField(sAttrStart4, mxLog);
					if (!(null == sStartSale || "".equals(sStartSale) || "NULL".equalsIgnoreCase(sStartSale))) {
						hmAdapletAttrNoMarket.put(ATTRIBUTE_SAFILO_ENOSAP_START_SALES, sStartSale);
						createDistrMarket(context, mxLog, sCountName, sTableName, sTimeStamp, sDate, sCode, hmAdapletAttrNoMarket, useTransaction);
					} else {
						String sErrMsg = new String("ATTR. <" + ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET4 + "> is not valorized <" + sAttrStart4 + ">. Impossible to procede.");
						if (_debug) {
							jpoLog(mxLog, sErrMsg);
						}
						createErrorDetail(context, mxLog, "NA", sTableName, sDate, SAFILO_SAP_ERROR_008, sErrMsg, "E", useTransaction);
					}
				}
			}
		} catch (Exception e) {
			// log the error
			String sExcDetails = "SAFILO_SAP_SendDistrMktToSAP execution error for BUS with ID " + sObjectId;
			sExcDetails += "\n(SAFILO_SAP with ID=" + sId + " and ID=" + sId + "):\n";
			sExcDetails += getExceptionDetails(e);

			// errors: rollback the transaction (?)
			try {
				//MqlUtil.mqlCommand(context, "abort transaction");
				if (isTrans) {
					MqlUtil.mqlCommand(context, "abort transaction", new String[]{});
				}
				sExcDetails += "\n=====\nSAFILO_SAP_SendToSAP rollback OK";
			} catch (Exception e1) {
				// log the error
				sExcDetails += "\n=====\nSAFILO_SAP_SendToSAP rollback error: " + getExceptionDetails(e1);
			}

			if (_debug) {
				jpoLog(mxLog, "sendToSAP s'e' rrroottttt!\n" + sExcDetails);
				mxLog.close();
			}
			throw new MatrixException(getLogDate() + " - " + "sendToSAP: " + sExcDetails);
		}
		if (_debug) {
			mxLog.close();
		}
		return iRetCode;
	}

	private boolean isCurrentDateAfterReleaseDate(Context context, DomainObject domObject) throws Exception {
		return isDateAfterReleaseDate(context, domObject, new Date());
	}

	private boolean isDateAfterReleaseDate(Context context, DomainObject domObject, Date date) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String targetDateText = sdf.format(date);

		if (date == null) {
			throw new IllegalArgumentException("Target date for comparison not specified");
		}
		String mxDate = domObject.getAttributeValue(context,ATTRIBUTE_SAFILO_MODELRELEASEDATE);

		if (StringUtils.isEmpty(mxDate)) {
			return false;
		}

		String releaseDateText = sdf.format(DateUtil.getJDateFromEmxDate(context, mxDate));

		return targetDateText.compareTo(releaseDateText) > 0;

	}


	public boolean createDistrMarket(
			Context context,
			BufferedWriter mxLog,
			String sCountName,
			String sTableName,
			String sTimeStamp,
			String sDate,
			String sCode,
			HashMap hmAdapletAttrNoMarket,
			boolean useTransaction)
			throws Exception {
		// initialize the return variable
		boolean isTrans = false;

		try {
			// start transaction
			if (useTransaction) {
				MqlUtil.mqlCommand(context, "start transaction", new String[]{});
				isTrans = true;
			}

			// get the ID
			String sDistrMktId = getCode(context, mxLog, sCountName);
			if (_debug) {
				jpoLog(mxLog, "Creating new DISTR.MARKET. with code=<" + sDistrMktId + "> for table " + sTableName);
			}

			// create the adaplet object
			BusinessObject boToSap = mxBus.create(context, "SAFILO_ENOSAP_" + sTableName, sDistrMktId, "", POLICY_SAFILO_ENOSAP, VAULT_ENOSAP);
			DomainObject doToSap = new DomainObject(boToSap);
			doToSap.open(context);
			doToSap.setAttributeValues(context, hmAdapletAttrNoMarket);
			doToSap.close(context);

			// no errors: commit the transaction
			//MqlUtil.mqlCommand(context, "commit transaction");
			if (useTransaction) {
				MqlUtil.mqlCommand(context, "commit transaction", new String[]{});
			}
		} catch (Exception e) {
			String sErrMsg = getExceptionDetails(e);
			if (useTransaction) {
				if (isTrans) {
					try {
						MqlUtil.mqlCommand(context, "abort transaction", new String[]{});
					} catch (FrameworkException e1) {
						sErrMsg += "\n ROLLBACK ERROR:\n" + getExceptionDetails(e1);
					}
				}
			}
			throw new MatrixException(sErrMsg);
		}
		return isTrans;
	}


	/**
	 * SC = Style_Code
	 * ST = Sku/Style Status
	 * SE = Sku EAN11
	 * <p>
	 * RETURN CODES
	 * SE ST SC
	 * 0  0  0 = 0 - OK
	 * 0  0  1 = 1 - SC       with errors
	 * 0  1  0 = 2 - ST       with errors
	 * 0  1  1 = 3 - SC,ST    with errors
	 * 1  0  0 = 4 - SE       with errors
	 * 1  0  1 = 5 - SE,SC    with errors
	 * 1  1  0 = 6 - SE,ST    with errors
	 * 1  1  1 = 7 - SE,ST,SC with errors
	 *
	 * @param context
	 * @param args
	 * @return
	 * @throws MatrixException
	 */
	public int updateFromSAP(Context context, String[] args) throws Exception {

		int iRetCode = 0;
		BufferedWriter mxLog = null;
		try {
			if (_debug) {
				MatrixLogWriter mtxw = new MatrixLogWriter(context, "FROM_SAP.log", "LOGWRITER", false);
				//mxLog = new BufferedWriter(new MatrixLogWriter(context));
				mxLog = new BufferedWriter(mtxw);
				jpoLog(mxLog, "updateFromSAP: Started ");
			}

			// set creator context
			emxContextUtil_mxJPO ShadowAgent = new emxContextUtil_mxJPO(context, null);
			ShadowAgent.pushContext(context, null);

			if (_debug) {
				jpoLog(mxLog, "updateFromSAP: context setted");
			}

			// get the states modifiable from SAP
			//StringList slSapAllowedModStates = (StringList)getStringListFromMapProgram(context, SAFILO_ENOSAP_ALLOWED_MOD_STATES);

			// rename the sent prototypes (if any)
			String sProtoRename = renamePrototypes(context, mxLog);
			jpoLog(mxLog, "updateFromSAP: ES_STYLE processed RC= <" + sProtoRename + ">");
			if ("KO".equals(sProtoRename)) {
				iRetCode = 1;
			}

			// update the SAFILO_SAP_Status (if any)
			String sUpdateSapStatus = updateSapStatus(context, mxLog);
			if (_debug) {
				jpoLog(mxLog, "updateFromSAP: SE_MD_SKU_STATUS processed RC= <" + sUpdateSapStatus + ">");
			}
			if ("KO".equals(sUpdateSapStatus)) {
				if (iRetCode == 1) {
					iRetCode = 3;
				} else {
					iRetCode = 2;
				}
			}

			// update the SAFILO_SAP_Status (if any)
			String sUpdateSkuEan11 = updateSkuEan11(context, mxLog);
			if (_debug) {
				jpoLog(mxLog, "updateFromSAP: ES_SKU (EAN11) processed RC= <" + sUpdateSkuEan11 + ">");
			}
			if ("KO".equals(sUpdateSkuEan11)) {
				if (iRetCode == 1) {
					iRetCode = 5;
				} else if (iRetCode == 2) {
					iRetCode = 6;
				} else if (iRetCode == 3) {
					iRetCode = 7;
				}
			}

			if (_debug) {
				jpoLog(mxLog, "updateFromSAP: finished with return code: " + iRetCode);
				mxLog.close();
			}
			return iRetCode;

		} catch (Exception e) {
			String sExcDetails = getExceptionDetails(e);
			if (_debug) {
				jpoLog(mxLog, "updateFromSAP s'e' roottt! " + sExcDetails);
				mxLog.close();
			}
			throw new MatrixException(getLogDate() + " - " + "updateFromSAP: " + sExcDetails);
		}
	}


	/**
	 * SC = Style_Code
	 * ST = Sku/Style Status
	 * SE = Sku EAN11
	 * <p>
	 * RETURN CODES
	 * SE ST SC
	 * 0  0  0 = 0 - OK
	 * 0  0  1 = 1 - SC       with errors
	 * 0  1  0 = 2 - ST       with errors
	 * 0  1  1 = 3 - SC,ST    with errors
	 * 1  0  0 = 4 - SE       with errors
	 * 1  0  1 = 5 - SE,SC    with errors
	 * 1  1  0 = 6 - SE,ST    with errors
	 * 1  1  1 = 7 - SE,ST,SC with errors
	 *
	 * @param context
	 * @param args
	 * @return
	 * @throws MatrixException
	 */
	public int addIdUpdatesSet(Context context, String[] args) throws Exception {

		int iRetCode = 0;
		String sObjectId = args[0];
		BufferedWriter mxLog = null;
		try {
			if (_debug) {
				MatrixLogWriter mtxw = new MatrixLogWriter(context, "TO_SAP.log", "LOGWRITER", false);
				//mxLog = new BufferedWriter(new MatrixLogWriter(context));
				mxLog = new BufferedWriter(mtxw);
				//jpoLog(mxLog, "Adding to set TOSAP bus " + sObjectId);
			}

			// set creator context
			emxContextUtil_mxJPO ShadowAgent = new emxContextUtil_mxJPO(context, null);
			ShadowAgent.pushContext(context, null);

			if (_debug) {
				//jpoLog(mxLog, "addIdUpdatesSet: context setted (" + context.getUser() + ")");
			}

			DomainObject doToSend = DomainObject.newInstance(context, sObjectId);
			StringList slBusSelects = new StringList(9);
			slBusSelects.add(SELECT_ID);
			slBusSelects.add(SELECT_TYPE);
			slBusSelects.add(SELECT_NAME);
			slBusSelects.add(SELECT_CURRENT);
			slBusSelects.add(SELECT_ATTRIBUTE_SAFILO_MODELCODE);
			slBusSelects.add(SELECT_ATTRIBUTE_SAFILO_UPC_CODE);
			slBusSelects.add(SELECT_KINDOF_EYEWEAR);
			slBusSelects.add(SELECT_KINDOF_SAFILO_SKU);
			slBusSelects.add(SELECT_KINDOF_SAFILO_LENS_TECH);

			// check the object
			boolean isToSend = false;
			Map mapBus = (Map) doToSend.getInfo(context, slBusSelects);
			String isKindOfEyewear = (String) mapBus.get(SELECT_KINDOF_EYEWEAR);
			String isKindOfSku = (String) mapBus.get(SELECT_KINDOF_SAFILO_SKU);
			String sBusType = (String) mapBus.get(SELECT_TYPE);
			if ("TRUE".equalsIgnoreCase(isKindOfEyewear)) {
				String sStyleName = (String) mapBus.get(SELECT_NAME);
				String sStyleCode = (String) mapBus.get(SELECT_ATTRIBUTE_SAFILO_MODELCODE);
				if (sStyleName.equals(sStyleCode)) {
					isToSend = true;
				}
			} else if ("TRUE".equalsIgnoreCase(isKindOfSku)) {
				String sSkuCurrent = (String) mapBus.get(SELECT_CURRENT);
				if (_debug) {
					//jpoLog(mxLog, "addIdUpdatesSet: sSkuCurrent <" + sSkuCurrent + ">");
				}
				if (STATE_ACTIVE.equals(sSkuCurrent) || STATE_CANCELLED.equalsIgnoreCase(sSkuCurrent)) {
					String sSkuUpcCode = (String) mapBus.get(SELECT_ATTRIBUTE_SAFILO_UPC_CODE);
					if (_debug) {
						//jpoLog(mxLog, "addIdUpdatesSet: sSkuUpcCode <" + sSkuUpcCode + ">");
					}
					if (sSkuUpcCode != null && !"".equals(sSkuUpcCode) && sSkuUpcCode.length() > 0) {
						isToSend = true;
					}
				}
			} else if (TYPE_SAFILO_COLORTECHNICAL.equals(sBusType) || TYPE_SAFILO_LENSCOLOR_TECHNICAL.equals(sBusType)) {
				String sBusCurrent = (String) mapBus.get(SELECT_CURRENT);
				if (!"Inspiration".equalsIgnoreCase(sBusCurrent)) {
					isToSend = true;
				}
			}


			// if the bus is to send then add it to the appropriate TOSAP set
			if (_debug) {
				jpoLog(mxLog, "addIdUpdatesSet: isToSend <" + isToSend + ">");
			}
			if (isToSend) {
				String sAttrName = MqlUtil.mqlCommand(context, "get env $1", new String[]{"ATTRNAME"});
				String sSetName = SAFILO_SAP_UPDATES_SET;
				if (_debug) {
					jpoLog(mxLog, "addIdUpdatesSet: sAttrName <" + sAttrName + ">");
				}
				if (sAttrName.startsWith("SAFILO_ReferenceMarket") || sAttrName.startsWith("SAFILO_StartSalesMonthMarket")) {
					//sSetName = SAFILO_SAP_UPDATES_DISTR_SET;
					sSetName = "";
					String sAttrValue = MqlUtil.mqlCommand(context, "get env $1", new String[]{"ATTRVALUE"});
					String sNewAttrValue = MqlUtil.mqlCommand(context, "get env $1", new String[]{"NEWATTRVALUE"});
					String sEnv = "\n\nENV:\n" + MqlUtil.mqlCommand(context, "list env", new String[]{}) + "\n";
					if (_debug) {
						jpoLog(mxLog, "addIdUpdatesSet: sAttrName     = <" + sAttrName + ">");
						jpoLog(mxLog, "addIdUpdatesSet: sAttrValue    = <" + sAttrValue + ">");
						jpoLog(mxLog, "addIdUpdatesSet: sNewAttrValue = <" + sNewAttrValue + ">");
						jpoLog(mxLog, "addIdUpdatesSet: " + sEnv);
					}

					// instantiate the object
					DomainObject doObj = DomainObject.newInstance(context, sObjectId);

					// get the "old" value
					//String sOldAttrValue = (String)doObj.getInfo(context, "attribute[" + sAttrName + "]");
					String sUpdateType = MqlUtil.mqlCommand(context, "get env $1", new String[]{sObjectId + "_" + sAttrName});
					if (sUpdateType == null || "".equals(sUpdateType) || sUpdateType.length() <= 0) {
						sUpdateType = "U";
						if (_debug) {
							jpoLog(mxLog, "addIdUpdatesSet: sUpdateType = <" + sUpdateType + "> (forced to)");
						}
					} else {
						if (_debug) {
							jpoLog(mxLog, "addIdUpdatesSet: sUpdateType = <" + sUpdateType + ">");
						}
					}

					String sAttrMkt = new String("");
					String sAttrStart = new String("");

					if (ATTRIBUTE_SAFILO_REFERENCEMARKET1.equals(sAttrName)) {
						if ("-".equals(sNewAttrValue) || "".equals(sNewAttrValue)) {
							if ("D".equals(sUpdateType)) {
								sNewAttrValue = MqlUtil.mqlCommand(context, "get env global $1", new String[]{sObjectId + "_" + sAttrName + "_VALUE"});
								cleanAttributeAndRelatedStartSalesMonth(context, sObjectId, ATTRIBUTE_SAFILO_REFERENCEMARKET1, ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET1);
							}
						}
						StringList slTemp = FrameworkUtil.split(sNewAttrValue, "-");
						String sCodeVal = (String) slTemp.get(0);
						sAttrMkt = sCodeVal.trim();
						sAttrStart = (String) doObj.getInfo(context, SELECT_ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET1);
						sAttrStart = getDateField(sAttrStart, mxLog);
					} else if (ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET1.equals(sAttrName)) {
						sAttrMkt = (String) doObj.getInfo(context, SELECT_ATTRIBUTE_SAFILO_REFERENCEMARKET1);
						StringList slTemp = FrameworkUtil.split(sAttrMkt, "-");
						String sCodeVal = (String) slTemp.get(0);
						sAttrMkt = sCodeVal.trim();
						sAttrStart = getDateField(sNewAttrValue, mxLog);
					} else if (ATTRIBUTE_SAFILO_REFERENCEMARKET2.equals(sAttrName)) {
						if ("-".equals(sNewAttrValue) || "".equals(sNewAttrValue)) {
							if ("D".equals(sUpdateType)) {
								sNewAttrValue = MqlUtil.mqlCommand(context, "get env global $1", new String[]{sObjectId + "_" + sAttrName + "_VALUE"});
								cleanAttributeAndRelatedStartSalesMonth(context, sObjectId, ATTRIBUTE_SAFILO_REFERENCEMARKET2, ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET2);
							}
						}
						StringList slTemp = FrameworkUtil.split(sNewAttrValue, "-");
						String sCodeVal = (String) slTemp.get(0);
						sAttrMkt = sCodeVal.trim();
						sAttrStart = (String) doObj.getInfo(context, SELECT_ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET2);
						sAttrStart = getDateField(sAttrStart, mxLog);
					} else if (ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET2.equals(sAttrName)) {
						sAttrMkt = (String) doObj.getInfo(context, SELECT_ATTRIBUTE_SAFILO_REFERENCEMARKET2);
						StringList slTemp = FrameworkUtil.split(sAttrMkt, "-");
						String sCodeVal = (String) slTemp.get(0);
						sAttrMkt = sCodeVal.trim();
						sAttrStart = getDateField(sNewAttrValue, mxLog);
					} else if (ATTRIBUTE_SAFILO_REFERENCEMARKET3.equals(sAttrName)) {
						if ("-".equals(sNewAttrValue) || "".equals(sNewAttrValue)) {
							if ("D".equals(sUpdateType)) {
								sNewAttrValue = MqlUtil.mqlCommand(context, "get env global $1", new String[]{sObjectId + "_" + sAttrName + "_VALUE"});
								cleanAttributeAndRelatedStartSalesMonth(context, sObjectId, ATTRIBUTE_SAFILO_REFERENCEMARKET3, ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET3);
							}
						}
						StringList slTemp = FrameworkUtil.split(sNewAttrValue, "-");
						String sCodeVal = (String) slTemp.get(0);
						sAttrMkt = sCodeVal.trim();
						sAttrStart = (String) doObj.getInfo(context, SELECT_ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET3);
						sAttrStart = getDateField(sAttrStart, mxLog);
					} else if (ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET3.equals(sAttrName)) {
						sAttrMkt = (String) doObj.getInfo(context, SELECT_ATTRIBUTE_SAFILO_REFERENCEMARKET3);
						StringList slTemp = FrameworkUtil.split(sAttrMkt, "-");
						String sCodeVal = (String) slTemp.get(0);
						sAttrMkt = sCodeVal.trim();
						sAttrStart = getDateField(sNewAttrValue, mxLog);
					} else if (ATTRIBUTE_SAFILO_REFERENCEMARKET4.equals(sAttrName)) {
						if ("-".equals(sNewAttrValue) || "".equals(sNewAttrValue)) {
							if ("D".equals(sUpdateType)) {
								sNewAttrValue = MqlUtil.mqlCommand(context, "get env global $1", new String[]{sObjectId + "_" + sAttrName + "_VALUE"});
								cleanAttributeAndRelatedStartSalesMonth(context, sObjectId, ATTRIBUTE_SAFILO_REFERENCEMARKET4, ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET4);
							}
						}
						StringList slTemp = FrameworkUtil.split(sNewAttrValue, "-");
						String sCodeVal = (String) slTemp.get(0);
						sAttrMkt = sCodeVal.trim();
						sAttrStart = (String) doObj.getInfo(context, SELECT_ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET4);
						sAttrStart = getDateField(sAttrStart, mxLog);
					} else if (ATTRIBUTE_SAFILO_STARTSALESMONTHMARKET4.equals(sAttrName)) {
						sAttrMkt = (String) doObj.getInfo(context, SELECT_ATTRIBUTE_SAFILO_REFERENCEMARKET4);
						StringList slTemp = FrameworkUtil.split(sAttrMkt, "-");
						String sCodeVal = (String) slTemp.get(0);
						sAttrMkt = sCodeVal.trim();
						sAttrStart = getDateField(sNewAttrValue, mxLog);
					}

					// get the NEWATTRVALUE and the ATTRVALUE
					boolean useTransaction = false;
					if (!"".equals(sAttrMkt) && "".equals(sAttrStart)) {
						if ("D".equals(sUpdateType)) {
							if (_debug) {
								jpoLog(mxLog, "addIdUpdatesSet: calling sendSingleDistrMktToSAP with sAttrMkt    = <" + sAttrMkt + "> and sAttrStart = <" + sAttrStart + ">");
							}
							sendSingleDistrMktToSAP(context, sObjectId, sUpdateType, useTransaction, sAttrMkt, sAttrStart);
						} else {
							if (_debug) {
								jpoLog(mxLog, "addIdUpdatesSet: skipping sAttrMkt    = <" + sAttrMkt + "> and sAttrStart = <" + sAttrStart + ">");
							}
						}
					} else {
						if (_debug) {
							jpoLog(mxLog, "addIdUpdatesSet: calling sendSingleDistrMktToSAP with sAttrMkt    = <" + sAttrMkt + "> and sAttrStart = <" + sAttrStart + ">");
						}
						sendSingleDistrMktToSAP(context, sObjectId, sUpdateType, useTransaction, sAttrMkt, sAttrStart);
					}
				} else if (sAttrName.startsWith("SAFILO_ProductDescriptionFor") || sAttrName.startsWith("SAFILO_ShippingNotes")) {
					sSetName = SAFILO_SAP_UPDATES_DESCR_SET;
				}
				if (_debug) {
					jpoLog(mxLog, "addIdUpdatesSet: to set <" + sSetName + "> (modified attribute <" + sAttrName + ">)");
				}

				if (!"".equals(sSetName)) {
					// check if the TOSAP set exists
					StringList slBusToAdd = new StringList(1);
					slBusToAdd.add(sObjectId);
					if (SetUtil.exists(context, sSetName)) {
						SetUtil.append(context, sSetName, slBusToAdd);
					} else {
						SetUtil.create(context, sSetName, slBusToAdd);
					}
				}
			}
			ShadowAgent.popContext(context, null);
			if (_debug) {
				jpoLog(mxLog, "sendUpdatesToSAP: finished" + iRetCode);
				mxLog.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			String sExcDetails = getExceptionDetails(e);
			String sDate = getDateForSAP();
			String sErrMsg = "ADD TO SET FAILED FOR BUS <" + sObjectId + ">: " + sExcDetails;
			System.err.println(sDate + " - " + sErrMsg);
			//createErrorDetail(context, mxLog, "NA", "NA", sDate, SAFILO_SAP_UPDATE_ERROR_110, sErrMsg, "E");
		}
		return iRetCode;
	}

	private void cleanAttributeAndRelatedStartSalesMonth(
			Context context,
			String sObjectId,
			String sAttrRefMarket,
			String sStartSalesMonthMarket) throws Exception {
		// set creator context
		emxContextUtil_mxJPO ShadowAgent = new emxContextUtil_mxJPO(context, null);
		ShadowAgent.pushContext(context, null);
		MqlUtil.mqlCommand(context, "trigger off", new String[]{});
		MqlUtil.mqlCommand(context, "modify bus $1 $2 $3 $4 $5", new String[]{sObjectId, sAttrRefMarket, "", sStartSalesMonthMarket, ""});
		MqlUtil.mqlCommand(context, "trigger on", new String[]{});
		ShadowAgent.popContext(context, null);
	}


	public String getTreeTag(String sLink, String sPath, String objectId, String sPage) {
		String sTagString = new String();
		String sParam = "objectId=" + objectId;
		sParam += "&suiteKey=ApparelAccelerator";
		sParam += "&emxSuiteDirectory=fao";
		String sHref =
				"emxTableColumnLinkClick(\'"
						+ sPath
						+ sPage
						+ "?"
						+ sParam
						+ "\', \'500\', \'500\', \'false\', \'content\')";
		sTagString = "<a  href=\"javascript:" + sHref + "\" ><b>" + sLink + "</b></a>";
		return sTagString;
	}

	/**
	 * @param context
	 * @param args
	 * @param busSelects
	 * @return
	 * @throws Exception
	 */
	public HashMap getInfoBus(Context context, String[] args, StringList busSelects)
			throws Exception {
		HashMap hmInfo = new HashMap();
		HashMap programMap = (HashMap) JPO.unpackArgs(args);

		MapList relBusObjPageList = (MapList) programMap.get("objectList");

		BusinessObjectWithSelectList busObjwsl = null;
		//----------------------------- MC
		//BufferedWriter mxLog = new BufferedWriter(new MatrixLogWriter(context));
		//JPOLog(mxLog, _debug, "relBusObjPageList "+relBusObjPageList);
		//-----------------------------
		if (relBusObjPageList != null) {
			String bol_array[] = new String[relBusObjPageList.size()];
			//String rel_array[] = new String[relBusObjPageList.size()];

			for (int i = 0; i < relBusObjPageList.size(); i++) {
				try {
					bol_array[i] = (String) ((HashMap) relBusObjPageList.get(i)).get("id");
				} catch (Exception ex) {
					bol_array[i] = (String) ((Hashtable) relBusObjPageList.get(i)).get("id");
				}
			}

			/* get set of data of type BusinessObjectWithSelectlist for all the business objects you passed in as a list*/
			busObjwsl = BusinessObject.getSelectBusinessObjectData(context, bol_array, busSelects);
		}

		hmInfo.put("hmInfoBus", busObjwsl);
		hmInfo.put("hmInfoRel", null);
		return hmInfo;
	}

	/**
	 * @param context
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public Object getEnoSapBusCode(Context context, String[] args) {
		String sTag = "";
		Vector vTag = new Vector();
		StringList slBusSelects = new StringList();
		slBusSelects.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MSG_ID);
		slBusSelects.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MSG_TABLE);

		try {
			HashMap hmInfo = (HashMap) getInfoBus(context, args, slBusSelects);

			//BufferedWriter mxLog = new BufferedWriter(new MatrixLogWriter(context));
			//JPOLog(mxLog, _debug, "hmInfo "+hmInfo);
			BusinessObjectWithSelectList busObjwsl = (BusinessObjectWithSelectList) hmInfo.get("hmInfoBus");
			for (int i = 0; i < busObjwsl.size(); i++) {
				// get SelectData from a BusinessObjectWithSelect object
				String sMsgId = (String) busObjwsl.getElement(i).getSelectData(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MSG_ID);
				if ("NA".equals(sMsgId)) {
					sTag = "NA, MSG_ID=NA";
				} else {
					try {
						String sMsgTable = (String) busObjwsl.getElement(i).getSelectData(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MSG_TABLE);
						if ("ES_STYLE".equals(sMsgTable) || "ZTPLM_STYLE".equals(sMsgTable)) {
							BusinessObject boStyle = new BusinessObject(TYPE_SAFILO_ENOSAP_ES_STYLE, sMsgId, "", "ENOSAP");
							DomainObject doStyle = DomainObject.newInstance(context, boStyle);
							StringList slBusSel = new StringList(2);
							slBusSel.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_BISMT);
							slBusSel.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR);
							Map mStyle = doStyle.getInfo(context, slBusSel);
							String sProtoCode = (String) mStyle.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_BISMT);
							String sStyleCode = (String) mStyle.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR);

							StringList slSelects = new StringList(1);
							slSelects.add(SELECT_ID);
							MapList mlEyewear = DomainObject.findObjects(context, // context
									TYPE_EYEWEAR,           // type pattern
									sStyleCode,             // name
									"*",                    // rev.
									"*",                    // owner pattern
									"*",                    // vault
									"revision == last",     // where clause
									true,                   // true, if the query should find subtypes of the given types
									slSelects);             // list of query select clause

							if ((mlEyewear != null) && (mlEyewear.size() == 1)) {
								Map mEyewear = (Map) mlEyewear.get(0);
								String sStyleId = (String) mEyewear.get(SELECT_ID);
								sTag = getTreeTag(sStyleCode, "../common/", sStyleId, "emxTree.jsp");
							} else {
								MapList mlPrototype = DomainObject.findObjects(context, // context
										TYPE_EYEWEAR,           // type pattern
										sProtoCode,             // name
										"*",                    // rev.
										"*",                    // owner pattern
										"*",                    // vault
										"revision == last",     // where clause
										true,                   // true, if the query should find subtypes of the given types
										slSelects);             // list of query select clause
								if ((mlPrototype != null) && (mlPrototype.size() == 1)) {
									Map mProto = (Map) mlPrototype.get(0);
									String sProtoId = (String) mProto.get(SELECT_ID);
									sTag = getTreeTag(sProtoCode, "../common/", sProtoId, "emxTree.jsp");
								} else {
									sTag = "MATNR=<" + sStyleCode + ">, BISMT=<" + sProtoCode + ">";
								}
							}
						} else if ("ES_SKU".equals(sMsgTable) || "ZTPLM_SKU".equals(sMsgTable)) {
							BusinessObject boSku = new BusinessObject(TYPE_SAFILO_ENOSAP_ES_SKU, sMsgId, "", "ENOSAP");
							DomainObject doSku = DomainObject.newInstance(context, boSku);
							StringList slBusSel = new StringList(2);
							slBusSel.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_PLM_SKU_CODE);
							slBusSel.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR);
							Map mEsSku = doSku.getInfo(context, slBusSel);
							String sSkuCode = (String) mEsSku.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_PLM_SKU_CODE);
							String sStyleCode = (String) mEsSku.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR);

							StringList slSelects = new StringList(1);
							slSelects.add(SELECT_ID);
							MapList mlSku = DomainObject.findObjects(context, // context
									TYPE_SAFILO_SKU,        // type pattern
									sSkuCode,               // name
									"*",                    // rev.
									"*",                    // owner pattern
									"*",                    // vault
									"revision == last",     // where clause
									true,                   // true, if the query should find subtypes of the given types
									slSelects);             // list of query select clause

							if ((mlSku != null) && (mlSku.size() == 1)) {
								Map mSku = (Map) mlSku.get(0);
								String sSkuId = (String) mSku.get(SELECT_ID);
								sTag = getTreeTag(sSkuCode, "../common/", sSkuId, "emxTree.jsp");
							} else {
								MapList mlEyewear = DomainObject.findObjects(context, // context
										TYPE_EYEWEAR,           // type pattern
										sStyleCode,             // name
										"*",                    // rev.
										"*",                    // owner pattern
										"*",                    // vault
										"revision == last",     // where clause
										true,                   // true, if the query should find subtypes of the given types
										slSelects);             // list of query select clause
								if ((mlEyewear != null) && (mlEyewear.size() == 1)) {
									Map mStyle = (Map) mlEyewear.get(0);
									String sStyleId = (String) mStyle.get(SELECT_ID);
									sTag = getTreeTag(sStyleCode, "../common/", sStyleId, "emxTree.jsp");
								} else {
									sTag = "MATNR=<" + sStyleCode + ">, PLM_SKU_CODE=<" + sSkuCode + ">";
								}
							}
						} else if ("ES_DISTR_MARKET".equals(sMsgTable) || "ZTPLM_MARKET".equals(sMsgTable)) {
							BusinessObject boDistrMkt = new BusinessObject(TYPE_SAFILO_ENOSAP_ES_DISTR_MARKET, sMsgId, "", "ENOSAP");
							DomainObject doDistrMkt = DomainObject.newInstance(context, boDistrMkt);
							StringList slBusSel = new StringList(2);
							slBusSel.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_SKU_CODE);
							slBusSel.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR);
							Map mEsDistrMkt = doDistrMkt.getInfo(context, slBusSel);
							String sSkuCode = (String) mEsDistrMkt.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_SKU_CODE);
							String sStyleCode = (String) mEsDistrMkt.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR);

							StringList slSelects = new StringList(1);
							slSelects.add(SELECT_ID);
							MapList mlSku = DomainObject.findObjects(context, // context
									TYPE_SAFILO_SKU,        // type pattern
									sSkuCode,               // name
									"*",                    // rev.
									"*",                    // owner pattern
									"*",                    // vault
									"revision == last",     // where clause
									true,                   // true, if the query should find subtypes of the given types
									slSelects);             // list of query select clause

							if ((mlSku != null) && (mlSku.size() == 1)) {
								Map mSku = (Map) mlSku.get(0);
								String sSkuId = (String) mSku.get(SELECT_ID);
								sTag = getTreeTag(sSkuCode, "../common/", sSkuId, "emxTree.jsp");
							} else {
								MapList mlEyewear = DomainObject.findObjects(context, // context
										TYPE_EYEWEAR,           // type pattern
										sStyleCode,             // name
										"*",                    // rev.
										"*",                    // owner pattern
										"*",                    // vault
										"revision == last",     // where clause
										true,                   // true, if the query should find subtypes of the given types
										slSelects);             // list of query select clause
								if ((mlEyewear != null) && (mlEyewear.size() == 1)) {
									Map mStyle = (Map) mlEyewear.get(0);
									String sStyleId = (String) mStyle.get(SELECT_ID);
									sTag = getTreeTag(sStyleCode, "../common/", sStyleId, "emxTree.jsp");
								} else {
									sTag = "MATNR=<" + sStyleCode + ">, PLM_SKU_CODE=<" + sSkuCode + ">";
								}
							}
						} else if ("ES_EXTENDED_DESCR".equals(sMsgTable)) {
							BusinessObject boExtDescr = new BusinessObject(TYPE_SAFILO_ENOSAP_ES_EXTENDED_DESCR, sMsgId, "", "ENOSAP");
							DomainObject doExtDescr = DomainObject.newInstance(context, boExtDescr);
							StringList slBusSel = new StringList(2);
							slBusSel.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_SKU_CODE);
							slBusSel.add(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR);
							Map mEsExtDescr = doExtDescr.getInfo(context, slBusSel);
							String sSkuCode = (String) mEsExtDescr.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_BISMT);
							String sStyleCode = (String) mEsExtDescr.get(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR);

							StringList slSelects = new StringList(1);
							slSelects.add(SELECT_ID);
							MapList mlSku = DomainObject.findObjects(context, // context
									TYPE_SAFILO_SKU,        // type pattern
									sSkuCode,               // name
									"*",                    // rev.
									"*",                    // owner pattern
									"*",                    // vault
									"revision == last",     // where clause
									true,                   // true, if the query should find subtypes of the given types
									slSelects);             // list of query select clause

							if ((mlSku != null) && (mlSku.size() == 1)) {
								Map mSku = (Map) mlSku.get(0);
								String sSkuId = (String) mSku.get(SELECT_ID);
								sTag = getTreeTag(sSkuCode, "../common/", sSkuId, "emxTree.jsp");
							} else {
								MapList mlEyewear = DomainObject.findObjects(context, // context
										TYPE_EYEWEAR,           // type pattern
										sStyleCode,             // name
										"*",                    // rev.
										"*",                    // owner pattern
										"*",                    // vault
										"revision == last",     // where clause
										true,                   // true, if the query should find subtypes of the given types
										slSelects);             // list of query select clause
								if ((mlEyewear != null) && (mlEyewear.size() == 1)) {
									Map mStyle = (Map) mlEyewear.get(0);
									String sStyleId = (String) mStyle.get(SELECT_ID);
									sTag = getTreeTag(sStyleCode, "../common/", sStyleId, "emxTree.jsp");
								} else {
									sTag = "MATNR=<" + sStyleCode + ">, PLM_SKU_CODE=<" + sSkuCode + ">";
								}
							}
						} else if ("ES_LENTECH".equals(sMsgTable)) {

						} else if ("ES_COLOR_TECH".equals(sMsgTable)) {

						} else if ("ES_ZTCOMMCOLOR".equals(sMsgTable)) {

						} else {
							if ("".equals(sMsgTable)) {
								sTag = "NA, MSG_TABLE NOT VALORIZED";
							} else {
								sTag = "NA, MSG_TABLE=" + sMsgTable + " NOT EXPECTED";
							}
						}
					} catch (Exception e) {
						String sExcDetails = getExceptionDetails(e);
						System.err.println("Exception in SAFILO_ENOSAP_UtilsJPO.getEnoSapBusCode " + sExcDetails);
						sTag = e.toString();
					}
				}

				vTag.add(sTag.trim());
			}
		} catch (Exception e) {
			String sExcDetails = getExceptionDetails(e);
			System.err.println("Exception in SAFILO_ENOSAP_UtilsJPO.getEnoSapBusCode " + sExcDetails);
		}
		return vTag;
	}


	/**
	 * @param context
	 * @param args
	 * @return
	 * @throws MatrixException
	 */
	public int setSkuCrossPlantStatusToZZ(Context context, String[] args) throws MatrixException {

		int iRetCode = 0;
		String sObjectId = args[0];
		//BufferedWriter mxLog = null;
		try {

			// set creator context
			emxContextUtil_mxJPO ShadowAgent = new emxContextUtil_mxJPO(context, null);
			ShadowAgent.pushContext(context, null);

			DomainObject doToUpdate = DomainObject.newInstance(context, sObjectId);
			doToUpdate.setAttributeValue(context, ATTRIBUTE_SAFILO_SKUCROSSPLANTSTATUS, "ZZ");
			ShadowAgent.popContext(context, null);

		} catch (Exception e) {
			e.printStackTrace();
			String sExcDetails = getExceptionDetails(e);
			String sDate = getDateForSAP();
			String sErrMsg = "SET \"ZZ\" FAILED FOR BUS <" + sObjectId + ">: " + sExcDetails;
			System.err.println(sDate + " - " + sErrMsg);
			//createErrorDetail(context, mxLog, "NA", "NA", sDate, SAFILO_SAP_UPDATE_ERROR_110, sErrMsg, "E");
		}
		return iRetCode;
	}


	/**
	 * @param context
	 * @param sAttrMkt
	 * @param sAttrStart
	 * @param args
	 * @return
	 * @throws MatrixException
	 */
	public int sendSingleDistrMktToSAP(Context context, String sObjectId, String sUpdateType, boolean useTransaction, String sAttrMkt, String sAttrStart) throws Exception {

		int iRetCode = 0;
		String sId = new String("");
		//String sJobId          = new String("");
		BufferedWriter mxLog = null;
		boolean isTrans = false;
		String sCountName = SAFILO_SAP_COUNTER_FOR_DISTR_MKT;
		String sTableName = "ES_DISTR_MARKET";

		try {
			if (_debug) {
				MatrixLogWriter mtxw = new MatrixLogWriter(context, "TOSAP.log", "LOGWRITER", false);
				mxLog = new BufferedWriter(mtxw);
			}

			emxContextUtil_mxJPO ShadowAgent = new emxContextUtil_mxJPO(context, null);
			ShadowAgent.pushContext(context, null);

			// get the time stamp
			String sTimeStamp = getTimestamp();

			Calendar cal = Calendar.getInstance();
			Date dayDate = cal.getTime();
			SimpleDateFormat fdata = new SimpleDateFormat("yyyyMMdd");
			String sDate = fdata.format(dayDate);
			//jpoLog(mxLog, sDate + " sendToSAP (DistrMkt): started on bus " + sObjectId + " with timestamp " + sTimeStamp);

			// retrieve object information and insert the records in the ENOSAP tables
			// get the attributes to check
			String sAttrsMappingMap = SAFILO_SAP_DISTR_MKT_ATTRS_MAPPING;
			StringList slAttrs = (StringList) getStringListFromMapProgram(context, sAttrsMappingMap);
			//HashMap    hmAttrsMap  = (HashMap)    getHashMapFromMapProgram(context, sAttrsMappingMap);
			StringList slDateAttrs = (StringList) getStringListFromMapProgram(context, SAFILO_SAP_DISTR_MKT_DATE_ATTRS);
			//StringList slFlagAttrs = (StringList) getStringListFromMapProgram(context, sFlagAttrsMap);
			HashMap hmAttrsMap = (HashMap) getHashMapFromMapProgram(context, sAttrsMappingMap);
			HashMap hmValLength = (HashMap) getHashMapFromMapProgram(context, SAFILO_SAP_ATTR_LENGTH_MAP);

			//jpoLog(mxLog, "\n slAttrs = " + slAttrs);

			// get the attributes values
			DomainObject domObject = new DomainObject(sObjectId);
			Map mAttrsVal = domObject.getInfo(context, slAttrs);
			String sCode = (String) domObject.getInfo(context, SELECT_NAME);

			//jpoLog(mxLog, "\n????\n mAttrsVal = " + mAttrsVal + "\n????\n");

			// define the hash map of the attributes
			HashMap hmAdapletAttr = new HashMap();
			for (int i = 0; i < slAttrs.size(); i++) {
				String sAttrName = (String) slAttrs.get(i);
				String sAttrVal = (String) mAttrsVal.get(sAttrName);
				if (sAttrVal == null) {
					continue;
				}
				if (slDateAttrs.contains(sAttrName)) {
					sAttrVal = getDateField(sAttrVal, mxLog);
				} else if (sAttrVal.indexOf('-') > 0) {
					if (!SELECT_ATTRIBUTE_SAFILO_RELEASE.equals(sAttrName)
							&& !"to[SAFILO_Products_SKU].businessobject.attribute[SAFILO_Release]".equals(sAttrName)
							&& !"to[SAFILO_Products_SKU].businessobject.to[Planned Product].businessobject.name".equals(sAttrName)) {
						StringList slTemp = FrameworkUtil.split(sAttrVal, "-");
						String sCodeVal = (String) slTemp.get(0);
						sAttrVal = sCodeVal.trim();
					} else {
						if (SELECT_ATTRIBUTE_SAFILO_RELEASE.equals(sAttrName) || "to[SAFILO_Products_SKU].businessobject.attribute[SAFILO_Release]".equals(sAttrName)) {
							sAttrVal = getReleaseDate(sAttrVal);
						}
					}
				}
				if ("".equals(sAttrVal)) {
					continue;
				}
				// check attribute length
				if (hmValLength.containsKey(hmAttrsMap.get(sAttrName))) {
					int iMaxValLength = Integer.parseInt((String) hmValLength.get(hmAttrsMap.get(sAttrName)));
					//jpoLog(mxLog, "Check attr. <" + sAttrName + " (" + hmAttrsMap.get(sAttrName) + ")> length. Value=<" + sAttrVal + "(" + sAttrVal.length() + ") + MAX=" + iMaxValLength);
					if (sAttrVal.length() > iMaxValLength) {
						String sWarnMsg = new String("ATTR. <" + sAttrName + "> TRUNCATED from <" + sAttrVal + "> (" + sAttrVal.length() + ") to <" + sAttrVal.substring(0, iMaxValLength) + ">");
						if (_debug) {
							jpoLog(mxLog, sWarnMsg);
						}
						//createErrorDetail(context, mxLog, "NA", sTableName, sDate, SAFILO_SAP_WARNING_104, sWarnMsg, "W", useTransaction);
						sAttrVal = sAttrVal.substring(0, iMaxValLength);
					}
				}
				hmAdapletAttr.put(hmAttrsMap.get(sAttrName), sAttrVal);
			}

			// SKU only: update the 2ND dimension (last 4 digit of the code)
			String sSkuCode = (String) mAttrsVal.get(SELECT_ATTRIBUTE_SAFILO_SKUCODE);
			if (sSkuCode != null && !"".equals(sSkuCode)) {
				String sSecDim = sSkuCode.substring(sSkuCode.length() - SAFILO_SAP_SKU_2ND_DIM_LENGTH);
				String sSkuCol = domObject.getInfo(context, SELECT_SKU_COLOR_FROM_SKU);
				//jpoLog(mxLog, "Updating SKU_COLOR with " + sSkuCol);
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_SKU_COLOR, sSkuCol);
				//jpoLog(mxLog, "Updating SKU_2ND_DIM with " + sSecDim);
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_SKU_2ND_DIM, sSecDim);
				//jpoLog(mxLog, "hmAdapletAttr after SKU_2ND_DIM " + hmAdapletAttr);
				// retrieve the PLM_RELEASE, SEASONAL_TYPE and SEASONAL_CODE from the related product
				StringList slForSku = new StringList(3);
				slForSku.add(SELECT_MODEL_RELEASE_DATE_FROM_SKU);
				slForSku.add(SELECT_SEASONAL_CODE_FROM_SKU);
				slForSku.add(SELECT_SEASONAL_TYPE_FROM_SKU);
				Map mForSku = domObject.getInfo(context, slForSku);
				if (_debug) {
					jpoLog(mxLog, "SKU.mForSku <" + mForSku + ">");
				}
				//
				String sModRelDate = (String) mForSku.get(SELECT_MODEL_RELEASE_DATE_FROM_SKU);
				String sSeasonCode = (String) mForSku.get(SELECT_SEASONAL_CODE_FROM_SKU);
				String sSeasonType = (String) mForSku.get(SELECT_SEASONAL_TYPE_FROM_SKU);
				if (sModRelDate != null && !"".equals(sModRelDate) && sModRelDate.length() > 0) {
					hmAdapletAttr.remove(ATTRIBUTE_SAFILO_ENOSAP_PLM_RELEASE);
					hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_PLM_RELEASE, getDateField(sModRelDate, mxLog));
				}
				//if ( sSeasonCode.indexOf('-') > 0 )
				//{
				//  StringList slTemp = FrameworkUtil.split(sSeasonCode, "-");
				//  String sCodeVal = (String)slTemp.get(0);
				//  sSeasonCode = sCodeVal.trim();
				//}
				hmAdapletAttr.remove(ATTRIBUTE_SAFILO_ENOSAP_PLM_SEASONAL_CODE);
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_PLM_SEASONAL_CODE, sSeasonCode);
				if (sSeasonType.indexOf('-') > 0) {
					StringList slTemp = FrameworkUtil.split(sSeasonType, "-");
					String sCodeVal = (String) slTemp.get(0);
					sSeasonType = sCodeVal.trim();
				}
				hmAdapletAttr.remove(ATTRIBUTE_SAFILO_ENOSAP_PLM_SEASONAL_TYPE);
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_PLM_SEASONAL_TYPE, sSeasonType);
			}
			// add the control attributes
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_JOB_USER, context.getUser());
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_READY_002);
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_STATUS_DATE, sDate);
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_MSG_TABLE, sTableName);
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_TIMESTAMP, sTimeStamp);
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_SENDER_SYSTEM, "ENO");
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_RECEIVER_SYSTEM, "SAP");
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE, sUpdateType);
			if (_debug) {
				jpoLog(mxLog, "hmAdapletAttr=" + hmAdapletAttr);
			}

			// at this point in the hmAdapletAttr we have all the attributes, let's loop on the ref.market
			HashMap hmAdapletAttrNoMarket = hmAdapletAttr;
			if (hmAdapletAttrNoMarket.containsKey(ATTRIBUTE_SAFILO_ENOSAP_ZMARKET)) {
				hmAdapletAttrNoMarket.remove(ATTRIBUTE_SAFILO_ENOSAP_ZMARKET);
			}
			if (hmAdapletAttrNoMarket.containsKey(ATTRIBUTE_SAFILO_ENOSAP_START_SALES)) {
				hmAdapletAttrNoMarket.remove(ATTRIBUTE_SAFILO_ENOSAP_START_SALES);
			}

			hmAdapletAttrNoMarket.put(ATTRIBUTE_SAFILO_ENOSAP_ZMARKET, sAttrMkt);
			hmAdapletAttrNoMarket.put(ATTRIBUTE_SAFILO_ENOSAP_START_SALES, sAttrStart);
			//jpoLog(mxLog, "1.."+ATTRIBUTE_SAFILO_ENOSAP_ZMARKET + "=" + sAttrMkt1);
			//jpoLog(mxLog, ATTRIBUTE_SAFILO_ENOSAP_START_SALES + "=" + getDateField(sAttrStart1, mxLog));
			createDistrMarket(context, mxLog, sCountName, sTableName, sTimeStamp, sDate, sCode, hmAdapletAttrNoMarket, useTransaction);
		} catch (Exception e) {
			// log the error
			String sExcDetails = "SAFILO_SAP_SendSingleDistrMktToSAP execution error for BUS with ID " + sObjectId;
			sExcDetails += "\n(SAFILO_SAP with ID=" + sId + " and ID=" + sId + "):\n";
			sExcDetails += getExceptionDetails(e);

			// errors: rollback the transaction (?)
			try {
				//MqlUtil.mqlCommand(context, "abort transaction");
				if (isTrans) {
					MqlUtil.mqlCommand(context, "abort transaction", new String[]{});
				}
				sExcDetails += "\n=====\nSAFILO_SAP_SendToSAP rollback OK";
			} catch (Exception e1) {
				// log the error
				sExcDetails += "\n=====\nSAFILO_SAP_SendToSAP rollback error: " + getExceptionDetails(e1);
			}

			if (_debug) {
				jpoLog(mxLog, "sendToSAP s'e' rrroottttt!\n" + sExcDetails);
				mxLog.close();
			}
			throw new MatrixException(getLogDate() + " - " + "sendToSAP: " + sExcDetails);
		}
		if (_debug) {
			mxLog.close();
		}
		return iRetCode;
	}


	/**
	 * SC = Style_Code
	 * ST = Sku/Style Status
	 * SE = Sku EAN11
	 * <p>
	 * RETURN CODES
	 * SE ST SC
	 * 0  0  0 = 0 - OK
	 * 0  0  1 = 1 - SC       with errors
	 * 0  1  0 = 2 - ST       with errors
	 * 0  1  1 = 3 - SC,ST    with errors
	 * 1  0  0 = 4 - SE       with errors
	 * 1  0  1 = 5 - SE,SC    with errors
	 * 1  1  0 = 6 - SE,ST    with errors
	 * 1  1  1 = 7 - SE,ST,SC with errors
	 *
	 * @param context
	 * @param args
	 * @return
	 * @throws MatrixException
	 */
	public int storePreviousMarketData(Context context, String[] args) throws Exception {

		int iRetCode = 0;
		String sObjectId = args[0];
		BufferedWriter mxLocalLog = null;
		try {
			// set creator context
			emxContextUtil_mxJPO ShadowAgent = new emxContextUtil_mxJPO(context, null);
			ShadowAgent.pushContext(context, null);

			DomainObject doToSend = DomainObject.newInstance(context, sObjectId);
			StringList slBusSelects = new StringList(8);
			slBusSelects.add(SELECT_ID);
			slBusSelects.add(SELECT_NAME);
			slBusSelects.add(SELECT_CURRENT);
			slBusSelects.add(SELECT_ATTRIBUTE_SAFILO_MODELCODE);
			slBusSelects.add(SELECT_ATTRIBUTE_SAFILO_UPC_CODE);
			slBusSelects.add(SELECT_KINDOF_EYEWEAR);
			slBusSelects.add(SELECT_KINDOF_SAFILO_SKU);
			slBusSelects.add(SELECT_KINDOF_SAFILO_LENS_TECH);

			// check the object
			boolean isToSend = false;
			Map mapBus = (Map) doToSend.getInfo(context, slBusSelects);
			String isKindOfEyewear = (String) mapBus.get(SELECT_KINDOF_EYEWEAR);
			String isKindOfSku = (String) mapBus.get(SELECT_KINDOF_SAFILO_SKU);
			if ("TRUE".equalsIgnoreCase(isKindOfEyewear)) {
				String sStyleName = (String) mapBus.get(SELECT_NAME);
				String sStyleCode = (String) mapBus.get(SELECT_ATTRIBUTE_SAFILO_MODELCODE);
				if (sStyleName.equals(sStyleCode)) {
					isToSend = true;
				}
			} else if ("TRUE".equalsIgnoreCase(isKindOfSku)) {
				String sSkuCurrent = (String) mapBus.get(SELECT_CURRENT);
				if (STATE_ACTIVE.equals(sSkuCurrent)) {
					String sSkuUpcCode = (String) mapBus.get(SELECT_ATTRIBUTE_SAFILO_UPC_CODE);
					if (sSkuUpcCode != null && !"".equals(sSkuUpcCode) && sSkuUpcCode.length() > 0) {
						isToSend = true;
					}
				}
			}

			// if the bus is to send then add it to the appropriate TOSAP set
			if (isToSend) {
				if (_debug) {
					MatrixLogWriter mtxw = new MatrixLogWriter(context, "STORE_PREVIOUS_FOR_SAP.log", "LOGWRITER", false);
					//mxLog = new BufferedWriter(new MatrixLogWriter(context));
					mxLocalLog = new BufferedWriter(mtxw);
					jpoLog(mxLocalLog, "Saving previous values for SAP " + sObjectId);
				}

				String sAttrName = MqlUtil.mqlCommand(context, "get env $1", new String[]{"ATTRNAME"});
				if (sAttrName.startsWith("SAFILO_ReferenceMarket") || sAttrName.startsWith("SAFILO_StartSalesMonthMarket")) {
					String sAttrValue = MqlUtil.mqlCommand(context, "get env $1", new String[]{"ATTRVALUE"});
					String sNewAttrValue = MqlUtil.mqlCommand(context, "get env $1", new String[]{"NEWATTRVALUE"});
					if (_debug) {
						jpoLog(mxLocalLog, "storePreviousMarketData: sAttrName     = <" + sAttrName + ">");
						jpoLog(mxLocalLog, "storePreviousMarketData: sAttrValue    = <" + sAttrValue + ">");
						jpoLog(mxLocalLog, "storePreviousMarketData: sNewAttrValue = <" + sNewAttrValue + ">");
					}

					// instantiate the object
					DomainObject doObj = DomainObject.newInstance(context, sObjectId);

					// get the "old" value
					String sOldAttrValue = (String) doObj.getInfo(context, "attribute[" + sAttrName + "]");
					if (_debug) {
						jpoLog(mxLocalLog, "storePreviousMarketData: sOldAttrValue = <" + sOldAttrValue + ">");
					}

					// get the modification type
					String sUpdateType = new String("");
					if (("".equals(sNewAttrValue) || "-".equals(sNewAttrValue)) && !"".equals(sOldAttrValue)) {
						sUpdateType = "D";
					} else if (!"".equals(sNewAttrValue) && "".equals(sOldAttrValue)) {
						sUpdateType = "C";
					} else {
						sUpdateType = "U";
					}

					if (_debug) {
						jpoLog(mxLocalLog, "storePreviousMarketData: storing update type <" + sUpdateType + "> for <" + sAttrName + "> of bus <" + sObjectId + ">");
					}
					MqlUtil.mqlCommand(context, "set env global $1 $2", new String[]{sObjectId + "_" + sAttrName, sUpdateType});
					MqlUtil.mqlCommand(context, "set env global $1 $2", new String[]{sObjectId + "_" + sAttrName + "_VALUE", sOldAttrValue});

				}
				if (_debug) {
					//String sEnv = "\n\nENV:\n" + MqlUtil.mqlCommand(context, "list env", new String[] { }) + "\n";
					//jpoLog(mxLocalLog, "storePreviousMarketData: " + sEnv);
					jpoLog(mxLocalLog, "storePreviousMarketData: finished" + iRetCode);
					mxLocalLog.close();
				}
			}
			ShadowAgent.popContext(context, null);
		} catch (Exception e) {
			e.printStackTrace();
			String sExcDetails = getExceptionDetails(e);
			String sDate = getDateForSAP();
			String sErrMsg = "STORE PREVIOUS MARKET DATA FAILED FOR BUS <" + sObjectId + ">: " + sExcDetails;
			System.err.println(sDate + " - " + sErrMsg);
			//createErrorDetail(context, mxLog, "NA", "NA", sDate, SAFILO_SAP_UPDATE_ERROR_110, sErrMsg, "E");
		}
		return iRetCode;
	}


	/**
	 * @param context
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public int sendColorTechToSAP(Context context, String[] args)
			throws Exception {
		int iRetCode = 0;

		String sObjectId = args[0];
		DomainObject doBus = DomainObject.newInstance(context, sObjectId);
		String sBusType = doBus.getInfo(context, SELECT_TYPE);
		if (TYPE_SAFILO_COLORTECHNICAL.equals(sBusType)) {
			iRetCode = sendToSAP(context, sObjectId, SAFILO_SAP_MANDATORY_COLOR_TECH_ATTRS, SAFILO_SAP_COUNTER_FOR_COLOR_TECH, SAFILO_SAP_COLOR_TECH_ATTRS_MAPPING, null, null, "ES_COLOR_TECH");
		}
		return iRetCode;
	}

	// ***** Start GC 20 07 2017 - method used in SAFILO_TriggerBase:promoteTecColorOnBomAdd
	public int sendColorTechToSAPnoTransaction(Context context, String[] args)
			throws Exception {
		int iRetCode = 0;

		String sObjectId = args[0];
		DomainObject doBus = DomainObject.newInstance(context, sObjectId);
		String sBusType = doBus.getInfo(context, SELECT_TYPE);
		if (TYPE_SAFILO_COLORTECHNICAL.equals(sBusType)) {
			iRetCode = sendToSAPnoTransaction(context, sObjectId, SAFILO_SAP_MANDATORY_COLOR_TECH_ATTRS, SAFILO_SAP_COUNTER_FOR_COLOR_TECH, SAFILO_SAP_COLOR_TECH_ATTRS_MAPPING, null, null, "ES_COLOR_TECH");
		}
		return iRetCode;
	}
	// ***** End GC 20 07 2017

	/**
	 * @param context
	 * @param args
	 * @return
	 * @throws MatrixException
	 */
	public int sendCommColorToSAP(Context context, String[] args) throws Exception {
		String sObjectId = args[0];
		return sendCommColorToSAP(context, sObjectId, true);
	}

	/**
	 * @param context
	 * @param args
	 * @return
	 * @throws MatrixException
	 */
	public int sendCommColorToSAP(Context context, String sObjectId, boolean isNew) throws Exception {
		int iRetCode = 0;
		String sCountName = SAFILO_SAP_COUNTER_FOR_COMM_COLOR;
		String sTableName = "ES_ZTCOMMCOLOR";
		BufferedWriter mxLog = null;

		try {
			if (_debug) {
				MatrixLogWriter mtxw = new MatrixLogWriter(context, "COMMCOLOR_TOSAP.log", "LOGWRITER", false);
				mxLog = new BufferedWriter(mtxw);
			}

			emxContextUtil_mxJPO ShadowAgent = new emxContextUtil_mxJPO(context, null);
			ShadowAgent.pushContext(context, null);

			// get the time stamp
			String sTimeStamp = getTimestamp();

			Calendar cal = Calendar.getInstance();
			Date dayDate = cal.getTime();
			SimpleDateFormat fdata = new SimpleDateFormat("yyyyMMdd");
			String sDate = fdata.format(dayDate);
			if (_debug) {
				jpoLog(mxLog, sDate + " sendToSAP (sendCommColorToSAP): started on bus " + sObjectId + " with timestamp " + sTimeStamp);
			}

			// retrieve object information and insert the records in the ENOSAP tables
			DomainObject domSku = DomainObject.newInstance(context, sObjectId);

			String sProductId = (String) domSku.getInfo(context, SELECT_PRODUCT_FROM_SKU);
			if (_debug) {
				jpoLog(mxLog, sDate + " sendToSAP (sendCommColorToSAP): sProductId <" + sProductId + ">");
			}
			String sSkuCode = (String) domSku.getInfo(context, SELECT_NAME);
			String sSkuColor = sSkuCode.substring(sSkuCode.length() - SAFILO_SAP_SKU_2ND_DIM_LENGTH - 3, sSkuCode.length() - SAFILO_SAP_SKU_2ND_DIM_LENGTH);
			String sSecDim = sSkuCode.substring(sSkuCode.length() - SAFILO_SAP_SKU_2ND_DIM_LENGTH);

			DomainObject domPrd = DomainObject.newInstance(context, sProductId);
			String sPrdCode = (String) domPrd.getInfo(context, SELECT_NAME);
			DomainObject domProduct = new DomainObject(sProductId);
			String sProdType = (String) domProduct.getInfo(context, SELECT_TYPE);
			if (_debug) {
				jpoLog(mxLog, sDate + " sendToSAP (sendCommColorToSAP): sProdType <" + sProdType + ">");
			}
			String sLensTechCode = new String("");
			if (TYPE_SAFILO_SUNGLASS.equals(sProdType)) {
				String sLenTechId = (String) domSku.getInfo(context, SELECT_ATTRIBUTE_SAFILO_TECHNICALCOLOR);
				if (_debug) {
					jpoLog(mxLog, sDate + " sendToSAP (sendCommColorToSAP): sLenTechId <" + sLenTechId + ">");
				}
				DomainObject domTechLens = DomainObject.newInstance(context, sLenTechId);
				domTechLens.open(context);
				sLensTechCode = (String) domTechLens.getInfo(context, SELECT_NAME);
				if (_debug) {
					jpoLog(mxLog, sDate + " sendToSAP (sendCommColorToSAP): sLensTechCode <" + sLensTechCode + ">");
				}
			}

			// define the hash map of the attributes
			HashMap hmAdapletAttr = new HashMap();

			// add the control attributes
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_JOB_USER, context.getUser());
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_STATUS, SAFILO_SAP_READY_002);
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_STATUS_DATE, sDate);
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_MSG_TABLE, sTableName);
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_TIMESTAMP, sTimeStamp);
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_SENDER_SYSTEM, "ENO");
			hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_RECEIVER_SYSTEM, "SAP");
			if (isNew) {
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE, "C");
			} else {
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE, "U");
			}

			// get the BOM elements
			MapList mlBom = getExpandSKU(context, sProductId, sObjectId);
			if (_debug) {
				jpoLog(mxLog, "sendCommColorToSAP - mlBom=<" + mlBom + ">");
			}

			String sBomLog = new String("");
			for (int x = 0; x < mlBom.size(); x++) {
				// [MATNR]           [varchar](20) NULL,
				// [SKU_COLOR]       [varchar](10) NULL,
				// [SKU_SIZE]        [varchar](10) NULL,
				// [ZSPAREPART]      [varchar] (5) NULL,
				// [COMPONENT_PROGR] [varchar] (3) NULL,
				// [ZTECHCOL]        [varchar](10) NULL,
				// [ZTECHLENS]       [varchar] (2) NULL,
				Hashtable tempBOM = (Hashtable) (mlBom.get(x));
				String sColorName = (String) tempBOM.get("ColorName");
				String sBomElem = (String) tempBOM.get(SELECT_NAME);
				sBomElem = sBomElem.substring(0, 2);
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_MATNR, sPrdCode);
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_SKU_COLOR, sSkuColor);
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_SKU_SIZE, sSecDim);
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_ZSPAREPART, sBomElem);
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_COMPONENT_PROGR, String.valueOf(x + 1));
				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_ZTECHCOL, sColorName);

				hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_ZTECHLENS, sLensTechCode);
				//START XU 08 08 2017
				if ("LD".equals(sBomElem) || "LE".equals(sBomElem) || "LI".equals(sBomElem) || "MA".equals(sBomElem) || "MD".equals(sBomElem) || "MI".equals(sBomElem)) {
					//END XU
					sColorName = "";

					hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_ZTECHCOL, "");
					hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_ZTECHLENS, sLensTechCode);
					sBomLog += "MATNR=<" + sPrdCode + "> SKU_COLOR=<" + sSkuColor + "> SKU_SIZE=<" + sSecDim + "> ZSPAREPART=<"
							+ sBomElem.substring(0, 2) + "> COMPONENT_PROGR=<" + x + "> ZTECHCOL=<> ZTECHLENS=<" + sLensTechCode + ">\n";


				} else {
					sLensTechCode = "";

					hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_ZTECHLENS, "");

					sBomLog += "MATNR=<" + sPrdCode + "> SKU_COLOR=<" + sSkuColor + "> SKU_SIZE=<" + sSecDim + "> ZSPAREPART=<"
							+ sBomElem.substring(0, 2) + "> COMPONENT_PROGR=<" + x + "> ZTECHCOL=<" + sColorName + "> ZTECHLENS=<>\n";
				}

				if (_debug) {

					String logmsg = "Elem = <" + sBomElem + "> Colorname = <" + sColorName + "> && sLensTechCode = <" + sLensTechCode + ">";
					jpoLog(mxLog, logmsg);

				}

				if ((sColorName == null || "".equals(sColorName)) && (sLensTechCode == null || "".equals(sLensTechCode))) {
					continue;
				}
				createZtCommColor(context, mxLog, sCountName, sTableName, sTimeStamp, sDate, hmAdapletAttr, false);
			}
			if (_debug) {
				jpoLog(mxLog, "BOM tranferred:\n" + sBomLog);
				mxLog.close();
			}

		} catch (Exception e) {
			String sExcDetails = getExceptionDetails(e);
			String sMsgErr = "Error while getting BOM: " + sExcDetails;
			throw new MatrixException(sMsgErr);
		}
		return iRetCode;
	}

	public MapList getExpandSKU(Context context, String sProductId, String sSKUId)
			throws Exception {
		StringList selectStmts = new StringList(4);
		selectStmts.addElement(SELECT_ID);
		selectStmts.addElement(SELECT_TYPE);
		selectStmts.addElement(SELECT_NAME);
		selectStmts.addElement(SELECT_LEVEL);

		StringList selectRelStmts = new StringList(4);
		selectRelStmts.addElement(SELECT_RELATIONSHIP_ID);
		selectRelStmts.addElement("attribute[Notes]");
		selectRelStmts.addElement("frommid[Selected Choice].to.name");
		selectRelStmts.addElement(SELECT_MARKETING_NAME_FROM_BOM);

		DomainObject domProductObj = new DomainObject(sProductId);
		DomainObject domSkuObj = new DomainObject(sSKUId);

		String sWhere = "";
		String sColWayForSKU = "";
		// String sColorDescr = "";
		MapList mlColorWayForSKU = domSkuObj.getRelatedObjects(
				context,
				RELATIONSHIP_SAFILO_SKU_CUSTOMERCHOICE,
				"*",
				selectStmts,
				selectRelStmts,
				false,
				true,
				(short) 1,
				sWhere,
				"",
				0);
		for (int x = 0; x < mlColorWayForSKU.size(); x++) {
			Hashtable tempHT = (Hashtable) (mlColorWayForSKU.get(x));
			String sColorName = (String) tempHT.get(SELECT_NAME);
			//String sColorId = (String) tempHT.get(SELECT_ID);
			//DomainObject domColor = new DomainObject(sColorId);
			//sColorDescr = domColor.getInfo(context, SELECT_DESCRIPTION);
			sColWayForSKU = sColorName;
		}
		if (sColWayForSKU.trim().equals("")) {
			sColWayForSKU = "NO_COLOR_VALUE_FOR_SKU";
		}
		MapList mlProductEBOM = domProductObj.getRelatedObjects(
				context,
				RELATIONSHIP_PRODUCT_EBOM,
				"*",
				selectStmts,
				selectRelStmts,
				false,
				true,
				(short) 1,
				sWhere,
				"",
				0);
		MapList mlFilteredEBOM = new MapList();
		MapList mlEBOM = new MapList();

		// PRODUCTEBOM --> collegato solo un Part a partire dal Product
		for (int i = 0; i < mlProductEBOM.size(); i++) {
			Hashtable tempHT = (Hashtable) (mlProductEBOM.get(i));
			String sPartId = (String) tempHT.get(SELECT_ID);
			DomainObject domPart = new DomainObject(sPartId);
			mlEBOM = domPart.getRelatedObjects(
					context,
					RELATIONSHIP_EBOM,
					"*",
					selectStmts,
					selectRelStmts,
					false,
					true,
					(short) 1,
					sWhere,
					"",
					0);
		}

		for (int y = 0; y < mlEBOM.size(); y++) {
			Hashtable tempHT = (Hashtable) (mlEBOM.get(y));
			// String sMaterialId = (String)tempHT.get(SELECT_ID);
			// String sMaterialName = (String)tempHT.get(SELECT_NAME);
			String sMaterialNote = (String) tempHT.get("attribute[Notes]");
			// String sColName = (String)tempHT.get("frommid.to.name");
			String sColName = "";
			String sMarketingName = "";
			try {
				sColName = tempHT.get("frommid[Selected Choice].to.name").toString();
				sMarketingName = tempHT.get(SELECT_MARKETING_NAME_FROM_BOM).toString();
			} catch (Exception exc) {
			}

			if (sColName != null && !sColName.equals("null") && sColName.contains(sColWayForSKU)) {
				String sMarketingId = "";
				if (sColName.contains(",")) {
					// Fare split
					// Rimuovo le parentesi quadre
					sColName = sColName.substring(1, (sColName.length() - 1));
					sMarketingName = sMarketingName.substring(1, (sMarketingName.length() - 1));
					String[] rowIdsList = sColName.split(",");
					for (int m = 0; m < rowIdsList.length; m++) {
						String sTempColName = (String) (rowIdsList[m]).trim();
						if (sTempColName.equals(sColWayForSKU)) {
							String[] rowMarksList = sMarketingName.split(",");
							//sMarketingId = (String) (rowMarksList[m]).trim();
							sMarketingId = (rowMarksList.length > m) ? (String) (rowMarksList[m]).trim() : "";
							break;
						}
					}
				} else {
					// valore singolo
					sMarketingId = sMarketingName.trim();
				}
				if (sMarketingId != null && !sMarketingId.equals("")) {
					DomainObject domTechnicalColor = new DomainObject(sMarketingId);
					tempHT.put("ColorName", domTechnicalColor.getInfo(context, "name"));
					tempHT.put("ColorDescr", domTechnicalColor.getAttributeValue(context, "SAFILO_ShortDescription"));
					tempHT.put("Family1", domTechnicalColor.getAttributeValue(context, "SAFILO_Family_1"));
					tempHT.put("Family2", domTechnicalColor.getAttributeValue(context, "SAFILO_Family_2"));
					tempHT.put("Material", domTechnicalColor.getAttributeValue(context, "SAFILO_Color_Material"));
					tempHT.put("Supplier", domTechnicalColor.getAttributeValue(context, "SAFILO_Supplier"));
					tempHT.put("SupplierCode", domTechnicalColor.getAttributeValue(context, "SAFILO_Supplier_Code"));
					tempHT.put("Effect", domTechnicalColor.getAttributeValue(context, "SAFILO_Effect"));
					tempHT.put("Note", sMaterialNote);
				} else {
					tempHT.put("ColorName", "");
					tempHT.put("ColorDescr", "");
					tempHT.put("Family1", "");
					tempHT.put("Family2", "");
					tempHT.put("Material", "");
					tempHT.put("Supplier", "");
					tempHT.put("SupplierCode", "");
					tempHT.put("Effect", "");
					tempHT.put("Note", sMaterialNote);
				}

				mlFilteredEBOM.add(tempHT);
			}
		}

		return mlFilteredEBOM;
	}


	public boolean createZtCommColor(
			Context context,
			BufferedWriter mxLog,
			String sCountName,
			String sTableName,
			String sTimeStamp,
			String sDate,
			HashMap hmAdapletZtCommColorAttr,
			boolean useTransaction)
			throws MatrixException {
		// initialize the return variable
		boolean isTrans = false;

		try {
			// start transaction
			if (useTransaction) {
				MqlUtil.mqlCommand(context, "start transaction", new String[]{});
				isTrans = true;
			}

			// get the ID
			String sCommColId = getCode(context, mxLog, sCountName);
			if (_debug) {
				jpoLog(mxLog, "Creating new COMM.COLOR with code=<" + sCommColId + "> for table " + sTableName);
			}

			// create the adaplet object
			BusinessObject boToSap = mxBus.create(context, "SAFILO_ENOSAP_" + sTableName, sCommColId, "", POLICY_SAFILO_ENOSAP, VAULT_ENOSAP);
			DomainObject doToSap = new DomainObject(boToSap);
			doToSap.open(context);
			doToSap.setAttributeValues(context, hmAdapletZtCommColorAttr);
			doToSap.close(context);

			// no errors: commit the transaction
			//MqlUtil.mqlCommand(context, "commit transaction");
			if (useTransaction) {
				MqlUtil.mqlCommand(context, "commit transaction", new String[]{});
				isTrans = false;
			}
		} catch (Exception e) {
			String sErrMsg = getExceptionDetails(e);
			if (useTransaction) {
				if (isTrans) {
					try {
						MqlUtil.mqlCommand(context, "abort transaction", new String[]{});
						isTrans = false;
					} catch (FrameworkException e1) {
						sErrMsg += "\n ROLLBACK ERROR:\n" + getExceptionDetails(e1);
					}
				}
			}
			throw new MatrixException(sErrMsg);
		}
		return isTrans;
	}


	public MapList getFromTableEsSku(Context context) {

		// Declare the JDBC objects.
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;

		MapList hmResults = new MapList();

		try {
			// Establish the connection.
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			con = DriverManager.getConnection(connectionUrl);

			// original where clause
			// (current == " + STATE_INSERTED + ")&&(" + SELECT_ATTRIBUTE_SAFILO_ENOSAP_STATUS + " == " + SAFILO_SAP_OK_000 + ")"

			// Create and execute the SQL select statement.
			String SQL = "SELECT TOP " + SELECT_MAX_NUM
					+ " [ROW_ID],[UPDATE_TYPE],[MATNR],[PLM_SKU_CODE],[ZMSTAE],[EAN11]"
					+ " FROM [" + sDatabaseName + "].[dbo].[ES_SKU]"
					+ " where ([STATUS]='000') or ([STATUS]='004')";

			// execute the query
			long startTime = System.nanoTime();
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(SQL);

			long endTime = System.nanoTime();
			long totalTime = endTime - startTime;
			// System.out.println(totalTime);

			// TimeUnit seconds = TimeUnit.SECONDS;
			TimeUnit nanosec = TimeUnit.NANOSECONDS;
			int rowcount = 0;
			if (rs.last()) {
				rowcount = rs.getRow();
				rs.beforeFirst(); // not rs.first() because the rs.next() below
				// will move on, missing the first element
			}
			if (_debug) {
				System.out.println("Selected " + rowcount + " rows");
				System.out.println("in " + nanosec.toMillis(totalTime) + " millis");
				System.out.println("which is " + nanosec.toSeconds(totalTime) + " seconds");
			}

			String sRowId = new String(""); // [ROW_ID]
			String sUpdateType = new String(""); // [UPDATE_TYPE]
			String sModelName = new String(""); // [MATNR]
			String sSkuName = new String(""); // [PLM_SKU_CODE]
			String sSapStatus = new String(""); // [ZMSTAE]
			String sEan11 = new String(""); // [EAN11]

			// Iterate through the data in the result set and display it.
			hmResults = new MapList(rowcount);
			int i = 0;
			while (rs.next()) {
				// System.out.println(rs.getString(1) + " " + rs.getString(2) +
				// " " + rs.getString(3) + " " + rs.getString(4) + " " +
				// rs.getString(5) + " " + rs.getString(6));
				// retrieve the required information
				sRowId = rs.getString(1);
				sUpdateType = rs.getString(2);
				sModelName = rs.getString(3);
				sSkuName = rs.getString(4);
				sSapStatus = rs.getString(5);
				sEan11 = rs.getString(6);

				HashMap hmRow = new HashMap();
				hmRow.put(SELECT_ID, sRowId);
				hmRow.put(SELECT_ATTRIBUTE_SAFILO_ENOSAP_EAN11, sEan11);
				hmRow.put(SELECT_ATTRIBUTE_SAFILO_ENOSAP_ZMSTAE, sSapStatus);
				hmRow.put(SELECT_ATTRIBUTE_SAFILO_ENOSAP_PLM_SKU_CODE, sSkuName);
				hmRow.put(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR, sModelName);
				hmRow.put(SELECT_ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE, sUpdateType);
				if (_debug) {
					System.out.println("BUSID     = " + sRowId);
					System.out.println("UPD_TYPE  = " + sUpdateType);
					System.out.println("EAN11     = " + sEan11);
					System.out.println("MOD.NAME  = " + sModelName);
					System.out.println("SAP.STAT  = " + sSapStatus);
					System.out.println("SKU.NAME  = " + sSkuName);
					System.out.println("hmRow=" + hmRow);
				}
				hmResults.add(i, hmRow);
				i++;
			}
		}

		// Handle any errors that may have occurred.
		catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception e) {
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
				}
			}
		}
		return hmResults;
	}


	public MapList getFromTableEsStyle(Context context) {

		// Declare the JDBC objects.
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;

		MapList hmResults = new MapList();

		try {
			// Establish the connection.
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			con = DriverManager.getConnection(connectionUrl);

			// Create and execute the SQL select statement.
			String SQL = "SELECT TOP " + SELECT_MAX_NUM
					+ " [ROW_ID],[STATUS],[UPDATE_TYPE],[BISMT],[MATNR]"
					+ " FROM [" + sDatabaseName + "].[dbo].[ES_STYLE]"
					+ " where ([STATUS]='000') or ([STATUS]='004')";
			// initialize the return variable
			long startTime = System.nanoTime();
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(SQL);

			long endTime = System.nanoTime();
			long totalTime = endTime - startTime;
			// System.out.println(totalTime);

			// TimeUnit seconds = TimeUnit.SECONDS;
			TimeUnit nanosec = TimeUnit.NANOSECONDS;
			int rowcount = 0;
			if (rs.last()) {
				rowcount = rs.getRow();
				rs.beforeFirst(); // not rs.first() because the rs.next() below
				// will move on, missing the first element
			}
			if (_debug) {
				System.out.println("Selected " + rowcount + " rows");
				System.out.println("in " + nanosec.toMillis(totalTime) + " millis");
				System.out.println("which is " + nanosec.toSeconds(totalTime) + " seconds");
			}

			String sRowId = new String(""); // [ROW_ID]
			String sStatus = new String(""); // [STATUS]
			String sUpdateType = new String(""); // [UPDATE_TYPE]
			String sProtName = new String(""); // [BISMT]
			String sModelName = new String(""); // [MATNR]

			// Iterate through the data in the result set and display it.
			hmResults = new MapList(rowcount);
			int i = 0;
			while (rs.next()) {
				// System.out.println(rs.getString(1) + " " + rs.getString(2) +
				// " " + rs.getString(3) + " " + rs.getString(4) + " " +
				// rs.getString(5) + " " + rs.getString(6));
				sRowId = rs.getString(1);
				sStatus = rs.getString(2);
				sUpdateType = rs.getString(3);
				sProtName = rs.getString(4);
				sModelName = rs.getString(5);
				HashMap hmRow = new HashMap();
				hmRow.put(SELECT_ID, sRowId);
				hmRow.put(SELECT_ATTRIBUTE_SAFILO_ENOSAP_STATUS, sStatus);
				hmRow.put(SELECT_ATTRIBUTE_SAFILO_ENOSAP_UPDATE_TYPE, sUpdateType);
				hmRow.put(SELECT_ATTRIBUTE_SAFILO_ENOSAP_BISMT, sProtName);
				hmRow.put(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR, sModelName);
				if (_debug) {
					System.out.println("BUSID     = " + sRowId);
					System.out.println("STATUS    = " + sStatus);
					System.out.println("UPD.TYPE  = " + sUpdateType);
					System.out.println("PROT.NAME = " + sProtName);
					System.out.println("MOD.NAME  = " + sModelName);
					System.out.println("hmRow=" + hmRow);
				}
				hmResults.add(i, hmRow);
				i++;
			}
		}

		// Handle any errors that may have occurred.
		catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception e) {
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
				}
			}
		}
		return hmResults;
	}


	public MapList getFromTableSeMdSkuStatus(Context context) {

		// Declare the JDBC objects.
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;

		MapList hmResults = new MapList();

		try {
			// Establish the connection.
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			con = DriverManager.getConnection(connectionUrl);

			// original where clause:
			// "(current == " + STATE_INSERTED + ")&&(" + SELECT_ATTRIBUTE_SAFILO_ENOSAP_STATUS + " == " + SAFILO_SAP_OK_000 + ")";

			// Create and execute the SQL select statement.
			String SQL = "SELECT TOP " + SELECT_MAX_NUM
					+ " [ROW_ID],[MATNR],[SKU_CODE],[MMSTA],[EAN11]"
					+ " FROM [" + sDatabaseName + "].[dbo].[SE_MD_SKU_STATUS]"
					+ " where ([STATUS]='000') or ([STATUS]='004')";

			// execute the query
			long startTime = System.nanoTime();
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(SQL);

			long endTime = System.nanoTime();
			long totalTime = endTime - startTime;

			// TimeUnit seconds = TimeUnit.SECONDS;
			TimeUnit nanosec = TimeUnit.NANOSECONDS;
			int rowcount = 0;
			if (rs.last()) {
				rowcount = rs.getRow();
				// not rs.first() because the rs.next() below
				// will move on, missing the first element
				rs.beforeFirst();
			}
			if (_debug) {
				System.out.println("Selected " + rowcount + " rows");
				System.out.println("in " + nanosec.toMillis(totalTime) + " millis");
				System.out.println("which is " + nanosec.toSeconds(totalTime) + " seconds");
			}

			String sRowId = new String(""); // [ROW_ID]
			String sModelName = new String(""); // [MATNR]
			String sSkuName = new String(""); // [SKU_CODE]
			String sSapStatus = new String(""); // [MMSTA]
			String sEan11 = new String(""); // [EAN11]

			// Iterate through the data in the result set and display it.
			hmResults = new MapList(rowcount);
			int i = 0;
			while (rs.next()) {
				// retrieve the required information
				sRowId = rs.getString(1);
				sModelName = rs.getString(2);
				sSkuName = rs.getString(3);
				sSapStatus = rs.getString(4);
				sEan11 = rs.getString(5);

				HashMap hmRow = new HashMap();
				hmRow.put(SELECT_ID, sRowId);
				hmRow.put(SELECT_ATTRIBUTE_SAFILO_ENOSAP_EAN11, sEan11);
				hmRow.put(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MMSTA, sSapStatus);
				hmRow.put(SELECT_ATTRIBUTE_SAFILO_ENOSAP_SKU_CODE, sSkuName);
				hmRow.put(SELECT_ATTRIBUTE_SAFILO_ENOSAP_MATNR, sModelName);
				if (_debug) {
					System.out.println("BUSID     = " + sRowId);
					System.out.println("EAN11     = " + sEan11);
					System.out.println("MOD.NAME  = " + sModelName);
					System.out.println("SAP.STAT  = " + sSapStatus);
					System.out.println("SKU.NAME  = " + sSkuName);
					System.out.println("hmRow=" + hmRow);
				}
				hmResults.add(i, hmRow);
				i++;
			}
		}

		// Handle any errors that may have occurred.
		catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception e) {
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
				}
			}
		}
		return hmResults;
	}


	public void createErrorDetail_OLD(
			Context context,
			BufferedWriter mxLog,
			String sMsgId,
			String sTableName,
			String sDate,
			String sReturnCode,
			String sError,
			String sErrorType,
			boolean bUseTransaction) {

		// get the time stamp
		String sTimeStamp = getTimestamp(false);

		// set the attributes
		HashMap hmAdapletAttr = new HashMap(8);
		//hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_JOB_USER, context.getUser());
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_MSG_TABLE, sTableName);
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_SENDER_SYSTEM, "ENO");
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_RECEIVER_SYSTEM, "SAP");
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_RETURN_CODE, sReturnCode);
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_MSG_ID, sMsgId);
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_ERROR_MSG, sError);
		hmAdapletAttr.put(ATTRIBUTE_SAFILO_ENOSAP_ERROR_TYPE, sErrorType);

		try {
			// start transaction
			if (bUseTransaction) {
				MqlUtil.mqlCommand(context, "start transaction", new String[]{});
			}

			// create the adaplet object
			BusinessObject boToSap = mxBus.create(context, TYPE_SAFILO_ENOSAP_ERROR_DETAIL_WRITE, sTimeStamp, "", POLICY_SAFILO_ENOSAP, VAULT_ENOSAP);
			DomainObject doToSap = new DomainObject(boToSap);
			doToSap.open(context);
			doToSap.setAttributeValues(context, hmAdapletAttr);
			doToSap.close(context);

			// no errors: commit the transaction
			if (bUseTransaction) {
				MqlUtil.mqlCommand(context, "commit transaction", new String[]{});
			}
			//jpoLog(mxLog, "SAFILO_ENOSAP_UtilsJPO.createErrorDetail - bus created with name = <" + sTimeStamp + ">");
		} catch (Exception e) {
			// Log the error to the file
			String sExcDet = getExceptionDetails(e);
			if (_debug) {
				jpoLog(mxLog, "\n!!! ERROR while creating ERROR_DETAIL with attributes:\n" + hmAdapletAttr + "\nDetails:\n" + sExcDet);
			}
			//e.printStackTrace();
			try {
				if (bUseTransaction) {
					MqlUtil.mqlCommand(context, "abort transaction", new String[]{});
				}
			} catch (FrameworkException e1) {
				String sExcDet1 = getExceptionDetails(e1);
				if (_debug) {
					jpoLog(mxLog, "\n!!! ERROR while aborting transaction.\nDetails:\n" + sExcDet1);
				}
			}
		}
	}

}
