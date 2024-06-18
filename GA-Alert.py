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

    global MINUTES_AGO
    global MINUTES
    global PROPERTY_DIMENSION
    global PROPERTY_METRIC

    global APPLICATION_DATA

    global WEBSITE_URL

    MINUTES_AGO = "minutesAgo" # this is defult value if value is not specified in ApplicationData file
    MINUTES = "02" # this is defult value if value is not specified in ApplicationData file
    PROPERTY_DIMENSION = "unifiedScreenName" # this is defult value if value is not specified in ApplicationData file
    PROPERTY_METRIC = "screenPageViews" # this is defult value if value is not specified in ApplicationData file

    f = open(
        "ApplicationData.json",
    )
    APPLICATION_DATA = json.load(f)
    
    WEBSITE_URL = "https://www.google.com/analytics/web/"

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"


def _get_thresold_traffic_data(reqPropertFilter):
    print("Start of _get_thresold_traffic_data >>>")
    print("reqPropertFilter = ", reqPropertFilter)
    hour = int(datetime.now().strftime("%H"))
    day = datetime.now().strftime("%A")
    for thresoldLimitDays in reqPropertFilter["thresoldLimit"]:
        if day.lower() in thresoldLimitDays.lower():
            for thresoldLimitDaysHous in reqPropertFilter["thresoldLimit"][
                thresoldLimitDays
            ]:
                if hour >= int(thresoldLimitDaysHous.split("-")[0]) and hour < int(
                    thresoldLimitDaysHous.split("-")[1]
                ):
                    print("thresoldLimitDays = ", thresoldLimitDays)
                    print("thresoldLimitDaysHous = ", thresoldLimitDaysHous)
                    print(
                        "thresoldLimitDaysHousLimt = ",
                        reqPropertFilter["thresoldLimit"][thresoldLimitDays][
                            thresoldLimitDaysHous
                        ],
                    )
                    print("End of _get_thresold_traffic_data <<<")
                    return reqPropertFilter["thresoldLimit"][thresoldLimitDays][
                        thresoldLimitDaysHous
                    ]

def _run_dynamic_query():
    emailMasterContent = dict()
    print("Start of _run_dynamic_query >>>")
    print(APPLICATION_DATA)
    day = datetime.now().strftime("%A")
    for application in APPLICATION_DATA:
        for emailConfigDays in application["emailConfig"]:
            print("emailConfigDays = ", emailConfigDays)
            if day.lower() in emailConfigDays.lower():
                print("emailConfigDayssss = ", application["emailConfig"][emailConfigDays]["isEmail"])
                if application["emailConfig"][emailConfigDays]["isEmail"] == True:
                    emailData = _process_report(
                        application["propertyId"],
                        application["propertyName"],
                        application["trafficThresoldLimit"],
                        application["propertyThresold"],
                    )
                    emailMasterContent = _dict_add(
                        emailMasterContent, application["propertyName"], emailData
                    )
        print(
            "******************************************************************************"
        )
    print("End of _run_dynamic_query <<<")
    return emailMasterContent


def _process_report(propertyId, propertyName, propertFilter, propertyThresold):
    # print("Start of _process_report >>>")
    return _report_based_on_filter(
        propertyId, propertyName, propertFilter, propertyThresold
    )
    # _check_website()
    print("End of _process_report <<<")


def _report_based_on_filter(
    propertyId, propertyName, propertFilter, propertyThresold
):
    print("Start of _report_based_on_filter >>>")
    print("propertyId = ", propertyId)
    print("propertyName = ", propertyName)
    print("propertFilter = ", propertFilter)
    print("propertyThresold = ", propertyThresold)
    hour = int(datetime.now().strftime("%H"))
    minutes = 0
    dimension = PROPERTY_DIMENSION
    metric = PROPERTY_METRIC
    day = datetime.now().strftime("%A")
    for propertyThresoldData in propertyThresold:
        print("propertyThresoldData = ", propertyThresoldData)
        if day.lower() in propertyThresoldData.lower():
            print(propertyThresold[propertyThresoldData])
            minutes = propertyThresold[propertyThresoldData]["propertyMinutes"] if "propertyMinutes" in propertyThresold[propertyThresoldData].keys() else MINUTES
            dimension = propertyThresold[propertyThresoldData]["propertyDimension"] if "propertyDimension" in propertyThresold[propertyThresoldData].keys() else PROPERTY_DIMENSION
            metric = propertyThresold[propertyThresoldData]["propertyMetric"] if "propertyMetric" in propertyThresold[propertyThresoldData].keys() else PROPERTY_METRIC
    print("minutes = ", minutes)
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
    print(request)
    response = client.run_realtime_report(request)
    print("End of _report_based_on_filter >>>")
    return _process_result(propertyName, propertFilter, response)


def _process_result(propertyName, propertFilters, response):
    print("Start of _process_result >>>")
    emailData = dict()
    
    print("response = ", response)
    for filters in propertFilters:
        print("filters = ", filters)
        thresoldValue = _get_thresold_traffic_data(propertFilters[filters])
        print("thresoldValue = ", thresoldValue)
        print("propertFilters[filters] = ", propertFilters[filters])
        if thresoldValue is not None:
            isLessThanCheck = False
            if propertFilters[filters]["check"] == "<":
                isLessThanCheck = True
            print("isLessThanCheck = ", isLessThanCheck)
            for filter in filters.split(","):
                print("filter = ", filter)
                isGreaterThanCheck = False
                isDimensionMatched = False
                isMetricCheck =  False
                screenEmailData = dict()
                for row in response.rows:
                    print("row = ", row)
                    if isLessThanCheck:
                        if filter.lower() in row.dimension_values[0].value.lower():
                            isDimensionMatched = True
                            if int(row.metric_values[0].value) > int(thresoldValue):
                                print("")
                                print(
                                    "isLessThanCheck TRUE = ",
                                    row.dimension_values[0].value
                                    + "  | = "
                                    + row.metric_values[0].value,
                                )
                                screenEmailData= _dict_add(screenEmailData, 'actual',thresoldValue)
                                screenEmailData= _dict_add(screenEmailData, 'current',row.metric_values[0].value)
                                # print("screenEmailData = ", screenEmailData)
                                emailData = _dict_add(
                                    emailData,
                                    filter,
                                    screenEmailData,
                                )
                            else:
                                isMetricCheck = True
                                print("int(row.metric_values[0].value) = ", int(row.metric_values[0].value))
                                print("thresoldValue = ", thresoldValue)
                            # isGreaterThanCheck = True
                            break
                        else:
                            print("isLessThanCheck TRUE filter.lower() = ", filter.lower())
                            print("isLessThanCheck TRUE ow.dimension_values[0].value.lower() = ", row.dimension_values[0].value.lower())
                    else:
                        if filter.lower() in row.dimension_values[0].value.lower():
                            isDimensionMatched = True
                            # isMetricCheck = True
                            if int(row.metric_values[0].value) < int(thresoldValue):
                                print(
                                    "isLessThanCheck Flase = ",
                                    row.dimension_values[0].value
                                    + "  = = "
                                    + row.metric_values[0].value,
                                )
                                screenEmailData= _dict_add(screenEmailData, 'actual',thresoldValue)
                                screenEmailData= _dict_add(screenEmailData, 'current',row.metric_values[0].value)
                                print("screenEmailData = ",screenEmailData)
                                emailData = _dict_add(
                                    emailData,
                                    filter,
                                    screenEmailData,
                                )
                            else:
                                isMetricCheck = True
                                print("int(row.metric_values[0].value) = ", int(row.metric_values[0].value))
                                print("thresoldValue = ", thresoldValue)
                            # isGreaterThanCheck = True
                            break
                        else:
                            print("isLessThanCheck FALSE filter.lower() = ", filter.lower())
                            print("isLessThanCheck FALSE ow.dimension_values[0].value.lower() = ", row.dimension_values[0].value.lower())

                print("isDimensionMatched = ", isDimensionMatched)
                print("isLessThanCheck = ", isMetricCheck)
                if isDimensionMatched is False:
                    if isLessThanCheck is False:
                        screenEmailData= _dict_add(screenEmailData, 'actual', thresoldValue)
                        screenEmailData= _dict_add(screenEmailData, 'current', '0')
                        print("screenEmailData = ",screenEmailData)
                        emailData = _dict_add(
                            emailData,
                            filter,
                            screenEmailData,
                        ) 
                # if isDimensionMatched:
                #     if isMetricCheck:
                #         print("isDimensionMatched = ", isDimensionMatched)
                #         print("isMetricCheck = ", isMetricCheck)
                # else:
                #     if isLessThanCheck is False:
                #         print("237 isDimensionMatched = ", isDimensionMatched)
                #         print("238 isMetricCheck = ", isMetricCheck)
                #         screenEmailData= _dict_add(screenEmailData, 'actual',thresoldValue)
                #         screenEmailData= _dict_add(screenEmailData, 'current','0')
                #         print("screenEmailData = ",screenEmailData)
                #         emailData = _dict_add(
                #             emailData,
                #             filter,
                #             screenEmailData,
                #         )
                
    print("emailData = ", emailData)
    print("End of _process_result <<<")
    return emailData

def _send_email(messageData, appName, emailConfig):
    print("Start of _send_email >>>")
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
    print("End of _send_email <<<")


def _dict_add(dictionary, key, value):
    print("Start of _dict_add >>>")
    print("dictionary = ", dictionary)
    print("key = ", key)
    print("value = ", value)
    temp = dictionary.copy()
    temp[key] = value
    print("temp = ", temp)
    print("End of _dict_add <<<")
    return temp


def _draft_email(emailMasterContent):
    print("Start of _draft_email >>>")
    print("emailMasterContent = ", emailMasterContent)
    isSendEmail = False
    
    for application in APPLICATION_DATA:
        html = ""
        tablehtml = ""
        table = ""
        if application["propertyName"] in emailMasterContent.keys():
            print("emailMasterContent[application[propertyName] = ", emailMasterContent[application["propertyName"]])
            for index, emailData in enumerate(
                emailMasterContent[application["propertyName"]]
            ):
                isSendEmail = True
                print("emailData = ", emailData)
                print(emailMasterContent[application["propertyName"]][emailData])
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
            print("application[propertyName] = ", application["emailConfig"])
            day = datetime.now().strftime("%A")
            for emailConfigDays in application["emailConfig"]:
                print("emailConfigDays = ", emailConfigDays)
                if day.lower() in emailConfigDays.lower():
                    print("application[emailConfig] = ", application["emailConfig"][emailConfigDays])
                    if isSendEmail:
                        _send_email(html, application["propertyName"], application["emailConfig"][emailConfigDays])
                    else:
                        print("isSendEmail = False Emai not triggered")

    print("End of _draft_email <<<")


def _check_website():
    response = requests.get(WEBSITE_URL, verify=True)
    print(response.status_code)
    soup = BeautifulSoup(response.content, "html.parser")
    print(soup.title)


if __name__ == "__main__":
    _load_global_values()
    emailMasterContent = _run_dynamic_query()
    _draft_email(emailMasterContent)
