#include <QCoreApplication>
#include <iostream>
#include <QStringList>
#include <QDate>
#include <qDebug>
#include <QFile>
#include <QTextStream>
#include <QTextCodec>
#include <QDir>

using namespace std;
void createTwoLevelTables(const QString& schemaName, const QString& tableName,const QStringList& deviceNumbers,
                          QDate startDate, QDate endDate, const QString& type, bool dateOffset)
{
    QString deviceTables;
    QString deviceTablesTrigger;

    QString dateTables;
    QString dateTablesTriggers;

    QString checkConstraintTriggers;


    //начало триггера для главной таблицы
    deviceTablesTrigger.append(QString("CREATE OR REPLACE FUNCTION %2.%1_insert()\n"
                                       "RETURNS TRIGGER AS $$\n"
                                       "BEGIN\n\n").arg(tableName).arg(schemaName));


    for(int deviceIt = 0; deviceIt < deviceNumbers.size(); deviceIt++)
    {
        // создание первого уровня таблиц по номерам приборов
        QString createDateTableText = QString("CREATE TABLE %3.%1_%2 (CHECK (pr_num=%2))  INHERITS (%3.%1) TABLESPACE bokzspace;\n")
                .arg(tableName)
                .arg(deviceNumbers[deviceIt])
                .arg(schemaName);

        deviceTables.append(createDateTableText);

        QString conditionName;

        if(deviceIt == 0)
        {
            conditionName = "IF";
        }
        else
        {
            conditionName = "ELSIF";
        }

        // заполнения тела триггера по номерам приборов

        QString deviceTriggerString = QString("%1 ( NEW.pr_num = %2) THEN\n"
                                              "INSERT INTO %4.%3_%2 VALUES (NEW.*);\n")
                .arg(conditionName).arg(deviceNumbers[deviceIt]).arg(tableName).arg(schemaName);

        deviceTablesTrigger.append(deviceTriggerString);

        // конец триггера для главной таблицы
        if(deviceIt == deviceNumbers.size() - 1)
        {

            QString endOfTrigger = QString("ELSE\n"
                                           "RAISE EXCEPTION\n"
                                           "'There is no table to insert data with this device number';\n"
                                           "END IF;\n"
                                           "RETURN NULL;\n"
                                           " END;\n"
                                           "$$\n"
                                           "LANGUAGE plpgsql;\n\n");
            deviceTablesTrigger.append(endOfTrigger);

            QString usingTrigger = QString("CREATE TRIGGER %1_insert_trigger\n"
                                         "BEFORE INSERT ON %2.%1\n"
                                         "FOR EACH ROW EXECUTE PROCEDURE %2.%1_insert();\n\n").arg(tableName).arg(schemaName);

            deviceTablesTrigger.append(usingTrigger);
        }


        // начало триггера таблиц по датам

        dateTablesTriggers.append(QString("CREATE OR REPLACE FUNCTION %2.%1_insert()\n"
                                          "RETURNS TRIGGER AS $$\n"
                                          "BEGIN\n\n").arg(tableName+"_"+deviceNumbers[deviceIt]).arg(schemaName));
        dateTables.append("\n\n\n");


        // делаем один триггер для всех таблиц внешнего уровня
        checkConstraintTriggers.append(QString("CREATE OR REPLACE FUNCTION %1.check_constraint_insert()\n"
                                               "RETURNS TRIGGER AS $$\n"
                                               "DECLARE\n "
                                               "result          integer;\n"
                                               "BEGIN\n "
                                               "EXECUTE 'SELECT 1 FROM ' || TG_TABLE_SCHEMA || '.' || TG_RELNAME || ' WHERE datetime = ' || quote_literal(NEW.datetime) ||';'"
                                               "INTO result;\n"
                                               "IF (result IS NULL) THEN\n"
                                               "RETURN NEW;\n"
                                               "END IF;\n"
                                               "RETURN NULL;\n"
                                               "END;\n"
                                               "$$\n"
                                               "LANGUAGE plpgsql;\n\n").arg(schemaName));


        QDate tstartDate = startDate.addMonths(-1);
        quint16 step;
        quint32 count;
        if (type == "m") {
            count = 12;
            step = 1;
        }
        else {
            count = (endDate.year() - startDate.year()) * 2;
            step = 6;
        }

        QDate fromDate = tstartDate;
        QDate toDate = fromDate;

        for (quint32 dIt = 0; dIt < count; dIt++)
        {
            toDate = toDate.addMonths(step);
            QDate toDateInsert;
            if (dateOffset)
            {
                toDateInsert = toDate;
            }
            else
            {
               toDateInsert = toDate.addMonths(-1);
            }

            QString createDateTableText = QString("CREATE TABLE %6.%1_%2 (PRIMARY KEY (datetime),\n"
                                                  "CHECK (datetime >= DATE '%3' AND datetime < DATE '%4'))  INHERITS (%6.%5) TABLESPACE bokzspace;\n")
                    .arg(tableName)
                    .arg(QString(toDateInsert.toString("MM") + toDateInsert.toString("yy") + "_" + deviceNumbers[deviceIt]))
                    .arg(fromDate.toString(Qt::ISODate))
                    .arg(toDate.toString(Qt::ISODate))
                    .arg(tableName + "_" + deviceNumbers[deviceIt])
                    .arg(schemaName);


            dateTables.append(createDateTableText);

            checkConstraintTriggers.append(QString("CREATE TRIGGER %1_insert_trigger\n"
                                                   "BEFORE INSERT ON %2.%1\n"
                                                   "FOR EACH ROW EXECUTE PROCEDURE %2.check_constraint_insert();\n\n\n")
                                           .arg(tableName + "_" + toDateInsert.toString("MM") + toDateInsert.toString("yy") + "_" + deviceNumbers[deviceIt])
                                           .arg(schemaName));


            if(dIt == 0)
            {
                conditionName = "IF";
            }
            else
            {
                conditionName = "ELSIF";
            }


            // заполняем тело триггера для таблиц второго уровня (по датам)

            //            QString dateTriggerString = QString("%1 ( NEW.datetime >= DATE '%2' AND NEW.datetime < DATE '%3' AND (SELECT COUNT(*) FROM %5.%4 "
            //                                                "WHERE datetime = NEW.datetime) = 0)  THEN\n"
            //                                                " INSERT INTO %5.%4 VALUES (NEW.*);\n")
            QString dateTriggerString = QString("%1 ( NEW.datetime >= DATE '%2' AND NEW.datetime < DATE '%3')  THEN\n"
                                                " INSERT INTO %5.%4 VALUES (NEW.*);\n")
                    .arg(conditionName)
                    .arg(fromDate.toString(Qt::ISODate))
                    .arg(toDate.toString(Qt::ISODate))
                    .arg(tableName + "_" + QString(toDateInsert.toString("MM") + toDateInsert.toString("yy") + "_"+deviceNumbers[deviceIt]))
                    .arg(schemaName);

            dateTablesTriggers.append(dateTriggerString);
            fromDate = fromDate.addMonths(step);

        }
        // заполняем конец триггера таблиц второго уровня

        QString endOfTrigger = QString("ELSE\n"
                                       "RAISE EXCEPTION\n"
                                       "'There is no table to insert data with this date interval';\n"
                                       "END IF;\n"
                                       "RETURN NULL;\n"
                                       " END;\n"
                                       "$$\n"
                                       "LANGUAGE plpgsql;\n\n");
        dateTablesTriggers.append(endOfTrigger);

        QString usingTrigger = QString("CREATE TRIGGER %1_insert_trigger\n"
                                       "BEFORE INSERT ON %2.%1\n"
                                       "FOR EACH ROW EXECUTE PROCEDURE %2.%1_insert();\n\n\n")
                .arg(tableName + "_" + deviceNumbers[deviceIt])
                .arg(schemaName);

        dateTablesTriggers.append(usingTrigger);

    }

    QFile deviceFile("results/" + tableName + "deviceTables.txt");


    if (!deviceFile.open(QFile::WriteOnly | QFile::Text | QFile::Truncate))
    {
        cout << "Не удалось открыть файл"<< endl;
        return;
    }
    QTextStream deviceStream(&deviceFile);

    QFile dateFile("results/" + tableName + "dateTables.txt");

    if (!dateFile.open(QFile::WriteOnly | QFile::Text | QFile::Truncate))
    {
        cout << "Не удалось открыть файл " + dateFile.fileName().toStdString() << endl;
        return;
    }
    QTextStream dateStream(&dateFile);

    QFile checkConstraintFile("results/" + tableName + "checkConstraint.txt");

    if (!checkConstraintFile.open(QFile::WriteOnly | QFile::Text | QFile::Truncate))
    {
        cout << "Не удалось открыть файл " + checkConstraintFile.fileName().toStdString()<< endl;
        return;
    }
    QTextStream checkConstraintStream(&checkConstraintFile);

    deviceStream << deviceTables << deviceTablesTrigger;
    dateStream << dateTables << dateTablesTriggers;
    checkConstraintStream << checkConstraintTriggers;

    deviceFile.close();
    dateFile.close();
    checkConstraintFile.close();
}

void createSHTMITables(const QString& schemaName, const QStringList& deviceNumbers)
{

    QString check;
    int i = 0;
    while (i < deviceNumbers.size() - 1)
    {
        check.append(QString("pr_num =") + deviceNumbers[i] + " OR ");
        i++;
    }
    check.append("pr_num = " + deviceNumbers.last());
    QString shtmi1;
    shtmi1.append(QString( " CREATE TABLE %1.shtmi1\n"
                           "  (\n"
                           " datetime timestamp without time zone NOT NULL,\n"
                           " time_pr double precision NOT NULL,\n"
                           " kc1 character varying(6) NOT NULL,\n"
                           " kc2 character varying(6) NOT NULL,\n"
                           " pr_num smallint NOT NULL,\n"
                           " post smallint NOT NULL,\n"
                           " foc real NOT NULL,\n"
                           " x_g real NOT NULL,\n"
                           " y_g real NOT NULL,\n"
                           " time_exp smallint NOT NULL,\n"
                           " mean_c smallint NOT NULL,\n"
                           " sigma_c smallint NOT NULL,\n"
                           " sum_const smallint NOT NULL,\n"
                           " sum_programm smallint NOT NULL,\n"
                           " x_ident smallint NOT NULL,\n"
                           " software_date smallint NOT NULL,\n"
                           " software_version smallint NOT NULL,\n"
                           " CONSTRAINT shtmi1_pkey PRIMARY KEY (datetime, pr_num),\n"
                           " CHECK (%2)\n"
                           "  );\n").arg(schemaName).arg(check));
    QString shtmi2;
    shtmi2.append(QString("CREATE TABLE %1.shtmi2\n"
                          "(\n"
                          " datetime timestamp without time zone NOT NULL,\n"
                          " time_pr double precision NOT NULL,\n"
                          " kc1 character varying(6) NOT NULL,\n"
                          " kc2 character varying(6) NOT NULL,\n"
                          " pr_num smallint NOT NULL,\n"
                          " post smallint NOT NULL,\n"
                          " num_usd smallint NOT NULL,\n"
                          " num_no smallint NOT NULL,\n"
                          " num_nosl smallint NOT NULL,\n"
                          " num_to smallint NOT NULL,\n"
                          " num_tosl smallint NOT NULL,\n"
                          " num_sl integer NOT NULL,\n"
                          " num_ec smallint[] NOT NULL,\n"
                          " CONSTRAINT shtmi2_pkey PRIMARY KEY (datetime, pr_num),\n"
                          " CHECK (%2)\n"
                          ");\n").arg(schemaName).arg(check));

    QFile shtmiFile("results/shtmiTables.txt");

    if (!shtmiFile.open(QFile::WriteOnly | QFile::Text | QFile::Truncate))
    {
        cout << "Не удалось открыть файл" + shtmiFile.fileName().toStdString() << endl;
        return;
    }
    QTextStream shtmiStream(&shtmiFile);
    shtmiStream << shtmi1 << shtmi2;
    shtmiFile.close();


}

void createOneLevelTable(const QString& schemaName,const QString& tableName, QDate startDate,
                         QDate endDate, const QString& type, bool dateOffset)
{
    QString dateTables;
    QString dateTablesTriggers;
    QString conditionName;
    QString checkConstraintTriggers;
    dateTablesTriggers.append(QString("CREATE OR REPLACE FUNCTION %2.%1_insert()\n"
                                      "RETURNS TRIGGER AS $$\n"
                                      "BEGIN\n\n").arg(tableName).arg(schemaName));


    // делаем один триггер для всех таблиц внешнего уровня
    checkConstraintTriggers.append(QString("CREATE OR REPLACE FUNCTION %1.check_constraint_insert()\n"
                                           "RETURNS TRIGGER AS $$\n"
                                           "DECLARE\n "
                                           "result          integer;\n"
                                           "BEGIN\n"
                                           "EXECUTE 'SELECT 1 FROM ' || TG_TABLE_SCHEMA || '.' || TG_RELNAME || ' WHERE datetime = ' || quote_literal(NEW.datetime) ||';'\n"
                                           "INTO result;\n"
                                           "IF (result IS NULL) THEN\n"
                                           "RETURN NEW;\n"
                                           "END IF;\n"
                                           "RETURN NULL;\n"
                                           "END;\n"
                                           "$$\n"
                                           "LANGUAGE plpgsql;\n\n").arg(schemaName));

    startDate = startDate.addMonths(-1);
    quint16 step;
    quint32 count;
    if (type == "m") {
        count = 12;
        step = 1;
    }
    else {
        count = (endDate.year() - startDate.year()) * 2;
        step = 6;
    }

    QDate fromDate = startDate;
    QDate toDate = fromDate;

    for (int dIt = 0; dIt < count; dIt++)
    {
        toDate = toDate.addMonths(step);

        QDate toDateInsert;
        if (dateOffset)
        {
            toDateInsert = toDate;
        }
        else
        {
           toDateInsert = toDate.addMonths(-1);
        }
        QString createDateTableText = QString("CREATE TABLE %6.%1_%2 (PRIMARY KEY (datetime),\n"
                                              "CHECK (datetime >= DATE '%3' AND datetime < DATE '%4'))  INHERITS (%6.%5) TABLESPACE bokzspace;\n")
                .arg(tableName)
                .arg(QString(toDateInsert.toString("MM") + toDateInsert.toString("yy")))
                .arg(fromDate.toString(Qt::ISODate))
                .arg(toDate.toString(Qt::ISODate))
                .arg(tableName)
                .arg(schemaName);



        dateTables.append(createDateTableText);

        checkConstraintTriggers.append(QString("CREATE TRIGGER %1_insert_trigger\n"
                                               "BEFORE INSERT ON %2.%1\n"
                                               "FOR EACH ROW EXECUTE PROCEDURE %2.check_constraint_insert();\n\n\n")
                                       .arg(tableName + "_" + toDateInsert.toString("MM") + toDateInsert.toString("yy"))
                                       .arg(schemaName));


        if(dIt == 0)
        {
            conditionName = "IF";
        }
        else
        {
            conditionName = "ELSIF";
        }


        // заполняем тело триггера для таблиц второго уровня (по датам)

        //        QString dateTriggerString = QString("%1 ( NEW.datetime >= DATE '%2' AND NEW.datetime < DATE '%3' AND (SELECT COUNT(*) FROM %5.%4 WHERE datetime = NEW.datetime) = 0) THEN\n"
        //                                            "INSERT INTO %5.%4 VALUES (NEW.*);\n")
        QString dateTriggerString = QString("%1 ( NEW.datetime >= DATE '%2' AND NEW.datetime < DATE '%3') THEN\n"
                                            "INSERT INTO %5.%4 VALUES (NEW.*);\n")
                .arg(conditionName)
                .arg(fromDate.toString(Qt::ISODate))
                .arg(toDate.toString(Qt::ISODate))
                .arg(tableName+"_"+QString(toDateInsert.toString("MM") + toDateInsert.toString("yy")))
                .arg(schemaName);

        dateTablesTriggers.append(dateTriggerString);

        fromDate = fromDate.addMonths(step);



    }

    QString endOfTrigger = QString("ELSE\n"
                                   "RAISE EXCEPTION\n"
                                   "'There is no table to insert data with this date interval';\n"
                                   "END IF;\n"
                                   "RETURN NULL;\n"
                                   " END;\n"
                                   "$$\n"
                                   "LANGUAGE plpgsql;\n\n");
    dateTablesTriggers.append(endOfTrigger);

    QString usingTrigger = QString("CREATE TRIGGER %1_insert_trigger\n"
                                   "BEFORE INSERT ON %2.%1\n"
                                   "FOR EACH ROW EXECUTE PROCEDURE %2.%1_insert();\n\n\n").arg(tableName).arg(schemaName);

    dateTablesTriggers.append(usingTrigger);

    QFile dateFile("results/" + tableName + "dateTables.txt");

    if (!dateFile.open(QFile::WriteOnly | QFile::Text | QFile::Truncate))
    {
        cout << "Не удалось открыть файл" + dateFile.fileName().toStdString()<< endl;
        return;
    }
    QTextStream dateStream(&dateFile);
    dateStream << dateTables << dateTablesTriggers << checkConstraintTriggers;
    dateFile.close();
}

int main()
{

    QDir().mkdir("results");
    QTextStream cout(stdout);
    QTextStream in (stdin);

    QTextCodec::setCodecForLocale(QTextCodec::codecForName("IBM 866"));
    cout << "Enter the schema name:" << endl;

    QString schemaName;
    in >> schemaName;

    cout << "Months or years? (m/y)" << endl;
    QString type;
    in >> type;

    cout << "<One level or two? (o/t)" << endl;
    QString levelType;
    in >> levelType;

    QString date;
    QDate startDate;
    QDate endDate;
    if (type == "m") {
        cout << "Enter the start date:" << endl;
        in >> date;
        startDate = QDate::fromString(date,Qt::ISODate);
    }
    else if (type == "y") {
        cout << "Enter the interval (like 2013-2017):" << endl;
        in >> date;
        QStringList splitted = date.split("-");
        if (splitted.count() != 2) {
            cout << "Wrong command" << endl;
            return 0;
        }
        startDate = QDate(splitted[0].toInt(), 1, 1);
        endDate = QDate(splitted[1].toInt(),1,1);
    }
    else
    {
        cout << "Wrong command" << endl;
        return 0;
    }

    cout << "Enter device numbers (separeted by commas):" << endl;
    QString deviceNumbersStr;
    in >> deviceNumbersStr;
    cout << "date offset? (y/n):" << endl;

    QString doffset;
    in >> doffset;
    bool dateOffset = false;
    if (doffset == "y")
        dateOffset = true;


    QStringList deviceNumbers = deviceNumbersStr.split(',');
    if (levelType == "t") {
        createTwoLevelTables(schemaName, "dtmi", deviceNumbers, startDate, endDate, type, dateOffset);
        createTwoLevelTables(schemaName, "orient", deviceNumbers, startDate, endDate, type, dateOffset);
    }
    if(levelType == "o") {
        createOneLevelTable(schemaName, "dtmi", startDate, endDate, type, dateOffset);
        createOneLevelTable(schemaName, "orient", startDate, endDate, type, dateOffset);
    }
    createSHTMITables(schemaName, deviceNumbers);
    createOneLevelTable(schemaName, "ksv", startDate, endDate, type, dateOffset);
    cout << "Finished sucssesfuly" << endl;


    return 0;
}
