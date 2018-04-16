/*
**   emxUtilBase
**
**   Copyright (c) 1992-2013 Dassault Systemes.
**   All Rights Reserved.
**   This program contains proprietary and trade secret information of MatrixOne,
**   Inc.  Copyright notice is precautionary only
**   and does not evidence any actual or intended publication of such program
**
*/

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.StringTokenizer;

import matrix.db.Context;
import matrix.db.JPO;
import matrix.db.MQLCommand;

import com.matrixone.apps.domain.util.EnoviaResourceBundle;
import com.matrixone.apps.domain.util.FrameworkException;
import com.matrixone.apps.domain.util.PersonUtil;
import com.matrixone.apps.domain.util.PropertyUtil;
import com.matrixone.apps.framework.ui.UINavigatorUtil;
/**
 * The <code>emxUtilBase</code> jpo contains UI Table Component methods.
 *
 * @version AEF 10.0.1.0 - Copyright (c) 2003, MatrixOne, Inc.
 */

public class emxUtilBase_mxJPO
{

    /**
     * Constructor.
     *
     * @param context the eMatrix <code>Context</code> object
     * @param args holds no arguments
     * @throws Exception if the operation fails
     */

    public emxUtilBase_mxJPO (Context context, String[] args) throws Exception
    {
        //if (!context.isConnected())
            //throw new Exception("not supported no desktop client");
    }

    /**
     * This method is executed if a specific method is not specified.
     *
     * @param context the eMatrix <code>Context</code> object
     * @param args holds no arguments
     * @return an int 0 for success and non-zero for failure
     * @throws Exception if the operation fails
     */

    public int mxMain(Context context, String[] args)  throws Exception
    {
        //if (!context.isConnected())
            //throw new Exception("not supported no desktop client");
        return 0;
    }

    /**
     * This utility method gets list of property names as an input
     * and returns ArrayList of admin names.
     *
     * @param context the eMatrix <code>Context</code> object
     * @param args holds an array of Strings
     * @return an ArrayList object
     * @throws Exception if the operation fails
     */

    public ArrayList getAdminNameFromProperties(Context context,  String[] args) throws Exception
    {
        // Registration Program Name.
        String regProgName = "eServiceSchemaVariableMapping.tcl";

        // return list for admin names
        ArrayList al = new ArrayList(args.length);

        // For each property name get admin name from registration program.
        String cmds[] = new String[1];
        StringBuffer cmdsBuffer = new StringBuffer(80);
        cmdsBuffer.append("print program '");
        cmdsBuffer.append(regProgName);
        cmdsBuffer.append("' select");
        for (int i = 0; i < args.length; i++)
        {
            cmdsBuffer.append(" property[");
            cmdsBuffer.append(args[i]);
            cmdsBuffer.append("].to");
        }
        cmdsBuffer.append(" dump |");
        cmds[0] = cmdsBuffer.toString();
        String adminTypeAndName = (String)(executeMQLCommands(context, cmds)).get(0);

        // Get only admin name from output of above command.
        StringTokenizer st1 = new StringTokenizer(adminTypeAndName, "|");
        while (st1.hasMoreTokens())
        {
            StringTokenizer st2 = new StringTokenizer(st1.nextToken());
            StringBuffer adminName = new StringBuffer(32);
            int count = 0;
            while (st2.hasMoreTokens())
            {
                String token = st2.nextToken();
                if (count != 0)
                {
                    adminName.append(" ");
                    adminName.append(token);
                }
                count++;
            }
            al.add((adminName.toString()).trim());
        }

        return al;
    }

    /**
     * This utility method gets list of property names of policy and states as an input
     * and returns ArrayList of state names.
     *
     * @param context the eMatrix <code>Context</code> object
     * @param args holds an array of Strings
     * @return an ArrayList object
     * @throws Exception if the operation fails
     */

    public ArrayList getStateNamesFromProperties(Context context,  String[] args) throws Exception
    {
        // Registration Program Name.
        String regProgName = "eServiceSchemaVariableMapping.tcl";

        // return list for admin names
        ArrayList al = new ArrayList(args.length/2);

        // For each property name get admin name from registration program.
        for (int i = 0; i < args.length/2; i++)
        {
            String adminName = null;

            if (args[i*2].startsWith("policy_"))
            {
                String props[] = new String[1];
                props[0] = args[i*2];
                adminName = (String)(getAdminNameFromProperties(context, props)).get(0);
            }
            else
            {
                adminName = args[i*2];
            }

            if (adminName.length() != 0)
            {
                String cmds[] = new String[1];
                StringBuffer cmdsBuffer = new StringBuffer(50);
                cmdsBuffer.append("print policy '");
                cmdsBuffer.append(adminName);
                cmdsBuffer.append("' select property[");
                cmdsBuffer.append(args[i*2+1]);
                cmdsBuffer.append("].value dump");
                cmds[0] = cmdsBuffer.toString();
                String value = (String)(executeMQLCommands(context, cmds)).get(0);
                al.add(value);
            }
            else
            {
                al.add(adminName.trim());
            }
        }

        return al;
    }

    /**
     * This utility method gets mql command as an input
     * and returns ArrayList of results after executing mql command.
     *
     * @param context the eMatrix <code>Context</code> object
     * @param args holds the following input arguments:
     *        i - a String contains command
     * @return an ArrayList object
     * @throws Exception if the operation fails
     * @deprecated V6R2014, use MqlUtil.mqlCommand instead.
     */

    public ArrayList executeMQLCommands(Context context,  String[] args) throws Exception
    {
        // return Arraylist
        ArrayList al = new ArrayList();

        // execute each command passed as input argument
        // throw exception if any one of them fails.
        MQLCommand mc = new MQLCommand();
        mc.open(context);
        for(int i = 0; i < args.length; i++)
        {
            mc.executeCommand(context, args[i]);
            String sError = mc.getError().trim();
            if (sError.length() != 0)
            {
                Exception e = new Exception(sError);
                throw e;
            }
            if(mc.getResult().endsWith("\n")){
                al.add(mc.getResult().substring(0,mc.getResult().length()-1));
            }else{
                al.add(mc.getResult());
            }
        }
        mc.close(context);

        return al;
    }

    /**
     * This utility method gets key and base property file name as an input
     * and returns internationalized string.
     *
     * @param context the eMatrix <code>Context</code> object
     * @param args holds the following input arguments:
     *         0 - a String contains baseFileName value
     *        1 - a String contains a key
     * @return a String
     * @throws Exception if the operation fails
     */

    public String getI18NString(Context context,  String[] args) throws Exception
    {
        // this is the country
        String strContry = "";
        // this is the string id of the localize tag, if there is one
        String strLanguage = "";

        // Get base file name
        String baseFileName = args[0];
        // Get key
        String key = args[1];

        // Get locale
        String mqlCmds[] = new String[1];
        mqlCmds[0] = "print language";
        try
        {
            // ArrayList retArr = (ArrayList)JPO.invoke(context, "emxUtil", null, "executeMQLCommands", mqlCmds, ArrayList.class);
            // String result = (String)retArr.get(0);
            String result = (String)(executeMQLCommands(context, mqlCmds)).get(0);

            StringTokenizer st1 = new StringTokenizer(result, "\n");
            String localeInfo = st1.nextToken();
            String locale = localeInfo.substring(localeInfo.indexOf('\'') + 1, localeInfo.lastIndexOf('\''));

            int idxDash = locale.indexOf('-');
            int idxComma = locale.indexOf(',');
            int idxSemiColumn = locale.indexOf(';');
            if ((idxComma == -1) && (idxSemiColumn == -1) && (idxDash == -1))
            {
                strLanguage = locale;
            }
            else if (idxDash != -1)
            {
                boolean cont = true;
                if ((idxComma < idxDash) && (idxComma != -1))
                {
                    if ((idxSemiColumn == -1) || (idxComma < idxSemiColumn))
                    {
                        strLanguage = locale.substring(0, idxComma);
                        cont = false;
                    }
                }
                if ((cont) && (idxSemiColumn < idxDash) && (idxSemiColumn != -1) )
                {
                    if ((idxComma == -1) || (idxComma > idxSemiColumn))
                    {
                        strLanguage = locale.substring(0, idxSemiColumn);
                        cont = false;
                    }
                }
                else if (cont)
                {
                    boolean sec2 = true;
                    StringTokenizer st = new StringTokenizer(locale, "-");
                    if (st.hasMoreTokens()) {
                        strLanguage = st.nextToken();
                        if (st.hasMoreTokens()) {
                            strContry = st.nextToken();
                        } else {
                            sec2 = false;
                        }
                    } else {
                        sec2 = false;
                    }
                    int idx = strContry.indexOf(',');
                    if (idx != -1)
                    {
                        strContry = strContry.substring(0,idx);
                    }
                    idx = strContry.indexOf(';');
                    if (idx != -1)
                    {
                        strContry = strContry.substring(0,idx);
                    }
                    if (!sec2) {
                        System.out.println("MATRIX ERROR - LOCAL INFO CONTAINS WRONG DATA");
                    }
                }
            }
            else
            {
                if ((idxComma != -1) && ((idxComma < idxSemiColumn) || (idxSemiColumn == -1)))
                {
                    strLanguage = locale.substring(0, idxComma);
                }
                else
                {
                    strLanguage = locale.substring(0, idxSemiColumn);
                }
            }
        }
        catch (Exception e)
        {
            strLanguage = "";
            strContry = "";
        }

        // Get Resource bundle.
        Locale loc = new Locale(strLanguage, strContry);
        try
        {
            ResourceBundle messages = EnoviaResourceBundle.getBundle(context, baseFileName, loc.getLanguage());
            return (messages.getString(key));
        }
        catch (Exception e)
        {
            return key;
        }
    }

    /**
     * This utility method clears the Person cache in the RMI VM.
     *
     * @param context the eMatrix <code>Context</code> object
     * @param args holds no arguments
     * @return an int: 0 for success and non-zero for failure
     * @throws Exception if the operation fails
     */
    public static int clearPersonCache(Context context, String[] args)  throws Exception
    {
        try
        {
            PersonUtil.clearCache(context);
        }
        catch (FrameworkException Ex)
        {
            throw Ex;
        }
        return 0;
    }

    /**
     * This utility method clears the Person property cache in the RMI VM.
     *
     * @param context the eMatrix <code>Context</code> object
     * @param args contains a Map with the following entries:
     *        userName - a String  of the person name
     *        propertyName - a String of the property name
     * @return an int: 0 for success and non-zero for failure
     * @throws Exception if the operation fails
     */

    public static int clearPersonCacheProperty(Context context, String[] args)  throws Exception
    {
       HashMap paramMap = (HashMap)JPO.unpackArgs(args);
       String userName = (String) paramMap.get("userName");
       String propertyName = (String) paramMap.get("propertyName");

        try
        {
            PersonUtil.clearUserCacheProperty(context, userName, propertyName);
        }
        catch (FrameworkException Ex)
        {
            throw Ex;
        }
        return 0;
    }

     /**
     * This utility method loads the Property cache in the RMI VM
     * @param context the eMatrix <code>Context</code> object
     * @param args contains no args, null could be passed
     * @throws Exception if the operation fails
     */
    public static void loadFrameworkProperty(Context context,String [] args) throws Exception
    {
        String sAllSuites = EnoviaResourceBundle.getProperty(context, "eServiceSuites.DisplayedSuites");
    }

     /**
     * This utility method takes field/column values and  autocorrect the mxlink values
     * @param context the eMatrix <code>Context</code> object
     * @param args contains all field values, uiType, and language string
     * @return String contains xml file with error mxlinks and autocorrected data
     * @throws Exception if the operation fails
     */

  public String validateMxLinkData(Context context,String [] args) throws Exception {

      String sMxLinkErrorData = "";
      String uiType= "";
      String languageStr="";
      StringBuffer returnXmlBuf = new StringBuffer(100);
      boolean flag=true;
      int errorMxLinkCount=0;
      HashMap fieldMap = new HashMap();
      Hashtable errorHt = new Hashtable();
      HashMap paramMap = (HashMap)JPO.unpackArgs(args);
      if(paramMap.containsKey("uiType")) {
          uiType=(String)paramMap.get("uiType");
          paramMap.remove("uiType");
      }
     if(paramMap.containsKey("language")) {
          languageStr=(String)paramMap.get("language");
          paramMap.remove("language");
      }

      fieldMap = UINavigatorUtil.validateEmbeddedURL(context,paramMap,languageStr);

      if(fieldMap.containsKey("errorMxlink")) {
          errorHt=(Hashtable)fieldMap.get("errorMxlink");
          fieldMap.remove("errorMxlink");
      }

      java.util.Set mxLinkErrorSet = (java.util.Set)errorHt.entrySet();
      Iterator iterator =mxLinkErrorSet.iterator();
      while(iterator.hasNext() && errorMxLinkCount<10) {
           Map.Entry mxlinkME = (Map.Entry)iterator.next();
          sMxLinkErrorData = sMxLinkErrorData+"\n"+(String)mxlinkME.getValue();
          errorMxLinkCount++;
      }
      if(errorMxLinkCount>=10){
		  String errMsg3 = EnoviaResourceBundle.getProperty(context, "emxFrameworkStringResource", new Locale(languageStr), "emxFramework.DynamicURL.ErrorMsg3");
		  sMxLinkErrorData=sMxLinkErrorData+errMsg3;
      }

      if(!"".equals(sMxLinkErrorData) && uiType.equalsIgnoreCase("structurebrowser")) {
          sMxLinkErrorData = sMxLinkErrorData.replaceAll("(?i)(mxLink\\\\s*:\\\\s*)","");
      }else if( !"".equals(sMxLinkErrorData)) {
    	  Locale locale = new Locale(languageStr);
		  String errMsg1 = EnoviaResourceBundle.getProperty(context, "emxFrameworkStringResource", locale, "emxFramework.DynamicURL.ErrorMsg1");
		  String errMsg2 = EnoviaResourceBundle.getProperty(context, "emxFrameworkStringResource", locale, "emxFramework.DynamicURL.ErrorMsg2"); 
		  sMxLinkErrorData = sMxLinkErrorData.replaceAll("(?i)(mxLink\\\\s*:\\\\s*)","");
          sMxLinkErrorData = errMsg1 + sMxLinkErrorData;
          sMxLinkErrorData = sMxLinkErrorData + errMsg2;
      }

      returnXmlBuf.append("<mxLinkRoot> <errorMsg> <![CDATA["+sMxLinkErrorData+"]]> </errorMsg>");
      java.util.Set fieldSet = (java.util.Set)fieldMap.entrySet();
      Iterator fieldItr = fieldSet.iterator();
      while(fieldItr.hasNext()) {
        Map.Entry me = (Map.Entry)fieldItr.next();
        returnXmlBuf.append("<mxField name=\"");
        returnXmlBuf.append(me.getKey() +"\">");
        returnXmlBuf.append(" <![CDATA["+me.getValue()+"]]> </mxField>");
      }
      returnXmlBuf.append("</mxLinkRoot>");
      return returnXmlBuf.toString();
      }
  
  
  /**
   * Update cache value for Admin property
   * @param context
   * @param args
   * String array of length 4
   * args[0] -> adminType 
   * args[1] -> adminName 
   * args[2] -> propertyName
   * args[3] -> propertyValue
   * 
   * e.g. {"person", "Test Everything", "IconMailLanguagePreference", "en"}
   * @throws FrameworkException
     * @deprecated V6R2013x, use updateAdminCache instead.
   */
  
   public void updateAdminCahce(Context context, String[] args) throws FrameworkException {
       updateAdminCache(context, args);
   }
   
  /**
   * Update cache value for Admin property
   * @param context
   * @param args
   * String array of length 4
   * args[0] -> adminType 
   * args[1] -> adminName 
   * args[2] -> propertyName
   * args[3] -> propertyValue
   * 
   * e.g. {"person", "Test Everything", "IconMailLanguagePreference", "en"}
   * @throws FrameworkException
   */
  
   public void updateAdminCache(Context context, String[] args) throws FrameworkException {
       if(args == null || args.length != 4) {
           return;
       }
       PropertyUtil.updateAdminProperty(context, args[0], args[1], args[2], args[3]);
   }
   

}
