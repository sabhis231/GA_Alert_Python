from datetime import datetime
import json
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
from bs4 import BeautifulSoup
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    Dimension,
    Metric,
    RunRealtimeReportRequest,
    FilterExpression,
    Filter,
    FilterExpressionList,
    NumericValue,
)


def _load_global_values():
    print("Start of _load_global_values >>>")

    global MINUTES_AGO
    global MINUTES
    global PROPERTY_DIMENSION
    global PROPERTY_METRIC

    global APPLICATION_DATA

    global WEBSITE_URL

    global DAY
    global HOUR

    global _TRUE

    MINUTES_AGO = "minutesAgo" # this is defult value if value is not specified in ApplicationData file
    MINUTES = "02" # this is defult value if value is not specified in ApplicationData file
    PROPERTY_DIMENSION = "unifiedScreenName" # this is defult value if value is not specified in ApplicationData file
    PROPERTY_METRIC = "screenPageViews" # this is defult value if value is not specified in ApplicationData file

    HOUR = int(datetime.now().strftime("%H"))
    DAY = datetime.now().strftime("%A")

    _TRUE = True

    f = open(
        "ApplicationData.json",
    )
    APPLICATION_DATA = json.load(f)
    
    WEBSITE_URL = "https://www.google.com/analytics/web/"

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"
    print("End of _load_global_values <<<")


def _get_thresold_traffic_data(reqPropertFilter):
    print("Start of _get_thresold_traffic_data >>>")
    for thresoldLimitDaysHous in reqPropertFilter["thresoldLimit"]:
        if HOUR >= int(thresoldLimitDaysHous.split("-")[0]) and HOUR < int(thresoldLimitDaysHous.split("-")[1]):
            print("End of _get_thresold_traffic_data <<<")
            return reqPropertFilter["thresoldLimit"][thresoldLimitDaysHous]

def _run_dynamic_query():
    emailMasterContent = dict()
    print("Start of _run_dynamic_query >>>")
    print(APPLICATION_DATA)
    day = datetime.now().strftime("%A")
    for application in APPLICATION_DATA:
        for appKeys in application:
            if day.lower() in appKeys.lower():
                appData = application[appKeys]
                isEnabled = application[appKeys]['isEnabled'] if "isEnabled" in application[appKeys].keys() else _TRUE
                if isEnabled:
                    emailData = _process_report(application["propertyId"], application["propertyName"], appData)
                    emailMasterContent = _dict_add(
                        emailMasterContent, application["propertyName"], emailData
                    )
        print(
            "******************************************************************************"
        )  
    print("End of _run_dynamic_query <<<")
    return emailMasterContent


def _process_report(propertyId, propertyName, appData):
    return _report_based_on_filter(
        propertyId, propertyName, appData
    )


def _report_based_on_filter(propertyId, propertyName, appData):
    print("Start of _report_based_on_filter >>>")
    if  "propertyThresold" in appData.keys():
        minutes = appData["propertyThresold"]["propertyMinutes"] if "propertyMinutes" in appData["propertyThresold"].keys() else MINUTES
        dimension = appData["propertyThresold"]["propertyDimension"] if "propertyDimension" in appData["propertyThresold"].keys() else PROPERTY_DIMENSION
        metric = appData["propertyThresold"]["propertyMetric"] if "propertyMetric" in appData["propertyThresold"].keys() else PROPERTY_METRIC
    else:
        minutes = MINUTES
        dimension = PROPERTY_DIMENSION
        metric = PROPERTY_METRIC
    client = BetaAnalyticsDataClient()
    request = RunRealtimeReportRequest(
        property=f"properties/{propertyId}",
        dimensions=[Dimension(name=dimension)],
        dimension_filter=FilterExpression(
            and_group=FilterExpressionList(
                expressions=[
                    # FilterExpression(
                    #     filter=Filter(
                    #         field_name=UNIFIED_SCREEN_NAME,
                    #         string_filter=Filter.StringFilter(
                    #             value=propertFilter.split("@")[0].split(":")[0],
                    #             match_type=Filter.StringFilter.MatchType(4)
                    #         ),
                    #     )
                    # ),
                    FilterExpression(
                        filter=Filter(
                            field_name=MINUTES_AGO,
                            numeric_filter=Filter.NumericFilter(
                                value=NumericValue(int64_value=minutes),
                                operation=Filter.NumericFilter.Operation(3),
                            ),
                        )
                    ),
                ]
            )
        ),
        metrics=[Metric(name=metric)],
    )
    response = client.run_realtime_report(request)
    print("End of _report_based_on_filter >>>")
    return _process_result(propertyName, appData, response)


def _process_result(propertyName, appData, response):
    print("Start of _process_result >>>")
    emailData = dict()
    trafficThresoldLimit = appData["trafficThresoldLimit"] if "trafficThresoldLimit" in appData.keys() else {}
    for filters in trafficThresoldLimit:
        thresoldValue = _get_thresold_traffic_data(trafficThresoldLimit[filters])
        if thresoldValue is not None:
            isLessThanCheck = False
            lessOrGreater = trafficThresoldLimit[filters]["check"]
            if lessOrGreater == "<":
                isLessThanCheck = True
            for filter in filters.split(","):
                isGreaterThanCheck = False
                isDimensionMatched = False
                isMetricCheck =  False
                screenEmailData = dict()
                for row in response.rows:
                    if isLessThanCheck:
                        if filter.lower() in row.dimension_values[0].value.lower():
                            isDimensionMatched = True
                            if int(row.metric_values[0].value) > int(thresoldValue):
                                screenEmailData = _dict_add(screenEmailData, 'actual',thresoldValue)
                                screenEmailData = _dict_add(screenEmailData, 'current',row.metric_values[0].value)
                                emailData = _dict_add(emailData, filter, screenEmailData)
                            else:
                                isMetricCheck = True
                                print("int(row.metric_values[0].value) = ", int(row.metric_values[0].value))
                                print("thresoldValue = ", thresoldValue)
                            break
                        else:
                            print("isLessThanCheck TRUE filter.lower() = ", filter.lower())
                            print("isLessThanCheck TRUE ow.dimension_values[0].value.lower() = ", row.dimension_values[0].value.lower())
                    else:
                        if filter.lower() in row.dimension_values[0].value.lower():
                            isDimensionMatched = True
                            if int(row.metric_values[0].value) < int(thresoldValue):
                                screenEmailData= _dict_add(screenEmailData, 'actual',thresoldValue)
                                screenEmailData= _dict_add(screenEmailData, 'current',row.metric_values[0].value)
                                emailData = _dict_add(emailData, filter, screenEmailData)
                            else:
                                isMetricCheck = True
                                print("int(row.metric_values[0].value) = ", int(row.metric_values[0].value))
                                print("thresoldValue = ", thresoldValue)
                            break
                        else:
                            print("isLessThanCheck FALSE filter.lower() = ", filter.lower())
                            print("isLessThanCheck FALSE ow.dimension_values[0].value.lower() = ", row.dimension_values[0].value.lower())
                if isDimensionMatched is False:
                    if isLessThanCheck is False:
                        screenEmailData = _dict_add(screenEmailData, 'actual', thresoldValue)
                        screenEmailData = _dict_add(screenEmailData, 'current', '0')
                        emailData = _dict_add(emailData, filter, screenEmailData)
    print("End of _process_result <<<")
    return emailData

def _send_email(messageData, appName, emailConfig):
    print("Start of _send_email >>>")
    isEmail = emailConfig['isEmail'] if "isEmail" in emailConfig.keys() else True
    if isEmail:
        message = MIMEMultipart("alternative")
        message["Subject"] = "[Important] Alert for " + appName 
        message["From"] = emailConfig["smtpUserName"]
        part = MIMEText(messageData, "html")
        message.attach(part)
        conn = smtplib.SMTP(emailConfig["smtpChannel"], emailConfig["smtpPort"])
        conn.ehlo()
        conn.starttls()
        conn.login(emailConfig["smtpUserName"], emailConfig["smtpUserKey"])
        conn.sendmail(emailConfig["emailFrom"], emailConfig["emailTo"], message.as_string())
        conn.quit()
    else:
        print("isEmail = isEmail is false email not triggered")

    
    print("End of _send_email <<<")


def _dict_add(dictionary, key, value):
    print("Start of _dict_add >>>")
    temp = dictionary.copy()
    temp[key] = value
    print("End of _dict_add <<<")
    return temp


def _draft_email(emailMasterContent):
    print("Start of _draft_email >>>")
    
    for application in APPLICATION_DATA:
        html = ""
        tablehtml = ""
        table = ""
        if application["propertyName"] in emailMasterContent.keys():
            for index, emailData in enumerate(emailMasterContent[application["propertyName"]]):
                if index % 2:
                    table += (
                        '<tr><td style="border: 1px solid #dddddd; text-align: left;padding: 8px;">'
                        + emailData
                        + '</td><td style="border: 1px solid #dddddd; text-align: left;padding: 8px;">'
                        + str(int(emailMasterContent[application["propertyName"]][emailData]['actual']))
                        + '</td><td style="border: 1px solid #dddddd; text-align: left;padding: 8px;">'
                        + str(int(emailMasterContent[application["propertyName"]][emailData]['current']))
                        + "</td></tr>"
                    )
                else:
                    table += (
                        '<tr style="background-color: #dddddd"><td style="border: 1px solid #dddddd; text-align: left;padding: 8px;">'
                        + emailData
                        + '</td><td style="border: 1px solid #dddddd; text-align: left;padding: 8px;">'
                        + str(int(emailMasterContent[application["propertyName"]][emailData]['actual']))
                        + '</td><td style="border: 1px solid #dddddd; text-align: left;padding: 8px;">'
                        + str(int(emailMasterContent[application["propertyName"]][emailData]['current']))
                        + "</td></tr>"
                    )
            tablehtml += (
                """
                <p>Please find the violations Pages in : """
                + application["propertyName"]
                + """</p>
                <table style="font-family: arial, sans-serif; border-collapse: collapse;width: 100%;">
                    <thead>
                        <tr>
                            <td style="border: 1px solid #dddddd; text-align: left;padding: 8px;">Name</td>
                            <td style="border: 1px solid #dddddd; text-align: left;padding: 8px;">Threshold</td>
                            <td style="border: 1px solid #dddddd; text-align: left;padding: 8px;">ActualCount</td>
                        </tr>
                    </thead>
                    <tbody>
                        """
                + table
                + """
                    </tbody>
                </table> """
            )
            html = (
            """
            <html><body><p>Hello, Team.</p>
            """
            + tablehtml
            + """
            <p>Regards,</p>
            <p>Automation Tool</p>
            </body></html>
            """
            )
            for appKey in application:
                if DAY.lower() in appKey.lower():
                    emailConfig = application[appKey]['emailConfig'] if "emailConfig" in application[appKey].keys() else None
                    if emailConfig is not None:
                        _send_email(html, application["propertyName"], emailConfig)
    print("End of _draft_email <<<")


def _check_website():
    response = requests.get(WEBSITE_URL, verify=True)
    print(response.status_code)
    soup = BeautifulSoup(response.content, "html.parser")
    print(soup.title)


if __name__ == "__main__":
    print("Start of __main__  at >>> ", datetime.now())
    _load_global_values()
    emailMasterContent = _run_dynamic_query()
    _draft_email(emailMasterContent)
    print("End of __main__ at >>>", datetime.now())
