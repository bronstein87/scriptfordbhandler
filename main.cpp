#include <QCoreApplication>
#include <iostream>
#include <QStringList>
#include <QDate>
#include <qDebug>
#include <QFile>
#include <QTextStream>
#include <QTextCodec>

using namespace std;
void createTwoLevelTables(const QString& tableName,const QStringList& deviceNumbers,
                          quint32 countOfMounth, const QDate& startDate)
{
    QString deviceTables;
    QString deviceTablesTrigger;

    QString dateTables;
    QString dateTablesTriggers;

    QString removeConstraint;



    //начало триггера для главной таблицы
    deviceTablesTrigger.append(QString("CREATE OR REPLACE FUNCTION %1_insert()\n"
                                       "RETURNS TRIGGER AS $$\n"
                                       "BEGIN\n\n").arg(tableName));


    for(int deviceIt=0;deviceIt<deviceNumbers.size();deviceIt++)
    {
        // создание первого уровня таблиц по номерам приборов
        QString createDateTableText=QString("CREATE TABLE %1_%2 (CHECK (pr_num=%2)) INHERITS (%1);\n")
                .arg(tableName)
                .arg(deviceNumbers[deviceIt]);

        deviceTables.append(createDateTableText);

        QString conditionName;

        if(deviceIt==0)
        {
            conditionName="IF";
        }
        else
        {
            conditionName="ELSIF";
        }

        // заполнения тела триггера по номерам приборов

        QString deviceTriggerString=QString("%1 ( NEW.pr_num=%2) THEN\n"
                                            "INSERT INTO %3_%2 VALUES (NEW.*);\n")
                .arg(conditionName).arg(deviceNumbers[deviceIt]).arg(tableName);

        deviceTablesTrigger.append(deviceTriggerString);

        // конец триггера для главной таблицы
        if(deviceIt==deviceNumbers.size()-1)
        {

            QString endOfTrigger=QString("ELSE\n"
                                         "RAISE EXCEPTION\n"
                                         "'There is no table to insert data with this device number';\n"
                                         "END IF;\n"
                                         "RETURN NULL;\n"
                                         " END;\n"
                                         "$$\n"
                                         "LANGUAGE plpgsql;\n\n");
            deviceTablesTrigger.append(endOfTrigger);

            QString usingTrigger=QString("CREATE TRIGGER %1_insert_trigger\n"
                                         "BEFORE INSERT ON %1\n"
                                         "FOR EACH ROW EXECUTE PROCEDURE %1_insert();\n\n").arg(tableName);

            deviceTablesTrigger.append(usingTrigger);
        }


        // начало триггера таблиц по датам

        dateTablesTriggers.append(QString("CREATE OR REPLACE FUNCTION %1_insert()\n"
                                          "RETURNS TRIGGER AS $$\n"
                                          "BEGIN\n\n").arg(tableName+"_"+deviceNumbers[deviceIt]));
        dateTables.append("\n\n\n");





        for(int mounthNumber=0;mounthNumber<countOfMounth;mounthNumber++)
        {

            QDate dateRangeEnd=startDate;
            dateRangeEnd.addMonths(mounthNumber);
            QString createDateTableText=QString("CREATE TABLE %1_%2 (PRIMARY KEY (datetime),\n"
                                                "CHECK (datetime >= DATE '%3' AND datetime < DATE '%4')) INHERITS (%5);\n")
                    .arg(tableName)
                    .arg(QString(startDate.addMonths(mounthNumber).toString("MM")+startDate.addMonths(mounthNumber).toString("yy")+"_"+deviceNumbers[deviceIt]))
                    .arg(startDate.addMonths(mounthNumber).toString(Qt::ISODate))
                    .arg(startDate.addMonths(mounthNumber+1).toString(Qt::ISODate))
                    .arg(tableName+"_"+deviceNumbers[deviceIt]);

            dateTables.append(createDateTableText);

            QString removeConstraintStr=QString("ALTER TABLE %1_%2 DROP CONSTRAINT %3_pr_num_check;\n")
                    .arg(tableName)
                    .arg(QString(startDate.addMonths(mounthNumber).toString("MM")+startDate.addMonths(mounthNumber).toString("yy")+"_"+deviceNumbers[deviceIt]))
                    .arg(tableName+"_"+deviceNumbers[deviceIt]);
            removeConstraint.append(removeConstraintStr);


            if(mounthNumber==0)
            {
                conditionName="IF";
            }
            else
            {
                conditionName="ELSIF";
            }


            // заполняем тело триггера для таблиц второго уровня (по датам)

            QString dateTriggerString=QString("%1 ( NEW.datetime >= DATE '%2' AND NEW.datetime < DATE '%3') THEN\n"
                                              "INSERT INTO %4 VALUES (NEW.*);\n")
                    .arg(conditionName)
                    .arg(startDate.addMonths(mounthNumber).toString(Qt::ISODate))
                    .arg(startDate.addMonths(mounthNumber+1).toString(Qt::ISODate))
                    .arg(tableName+"_"+QString(startDate.addMonths(mounthNumber).toString("MM")+startDate.addMonths(mounthNumber).toString("yy")+"_"+deviceNumbers[deviceIt]));

            dateTablesTriggers.append(dateTriggerString);


            // заполняем конец триггера таблиц второго уровня
            if(mounthNumber==countOfMounth-1)
            {
                QString endOfTrigger=QString("ELSE\n"
                                             "RAISE EXCEPTION\n"
                                             "'There is no table to insert data with this date interval';\n"
                                             "END IF;\n"
                                             "RETURN NULL;\n"
                                             " END;\n"
                                             "$$\n"
                                             "LANGUAGE plpgsql;\n\n");
                dateTablesTriggers.append(endOfTrigger);

                QString usingTrigger=QString("CREATE TRIGGER %1_insert_trigger\n"
                                             "BEFORE INSERT ON %1\n"
                                             "FOR EACH ROW EXECUTE PROCEDURE %1_insert();\n\n\n").arg(tableName+"_"+deviceNumbers[deviceIt]);

                dateTablesTriggers.append(usingTrigger);
            }


        }
    }

    QFile deviceFile("deviceTables.txt");


    if (!deviceFile.open(QFile::WriteOnly | QFile::Text | QFile::Truncate))
    {
        cout<<"Не удалось открыть файл"<<endl;
        return;
    }
    QTextStream deviceStream(&deviceFile);

    QFile dateFile("dateTables.txt");

    if (!dateFile.open(QFile::WriteOnly | QFile::Text | QFile::Truncate))
    {
        cout<<"Не удалось открыть файл"<<endl;
        return;
    }
    QTextStream dateStream(&dateFile);

    QFile removeConstraintFile("removeConstraint.txt");

    if (!removeConstraintFile.open(QFile::WriteOnly | QFile::Text | QFile::Truncate))
    {
        cout<<"Не удалось открыть файл"<<endl;
        return;
    }
    QTextStream removeConstraintStream(&removeConstraintFile);

    deviceStream<<deviceTables<<deviceTablesTrigger;
    dateStream<<dateTables<<dateTablesTriggers;
    removeConstraintStream<<removeConstraint;

    deviceFile.close();
    dateFile.close();
    removeConstraintFile.close();
}


void createOneLevelTable(const QString& tableName,quint32 countOfMounth, const QDate& startDate)
{
    QString dateTables;
    QString dateTablesTriggers;
    QString conditionName;
    dateTablesTriggers.append(QString("CREATE OR REPLACE FUNCTION %1_insert()\n"
                                      "RETURNS TRIGGER AS $$\n"
                                      "BEGIN\n\n").arg(tableName));

    for(int mounthNumber=0;mounthNumber<countOfMounth;mounthNumber++)
    {

        QDate dateRangeEnd=startDate;
        dateRangeEnd.addMonths(mounthNumber);
        QString createDateTableText=QString("CREATE TABLE %1_%2 (PRIMARY KEY (datetime),\n"
                                            "CHECK (datetime >= DATE '%3' AND datetime < DATE '%4')) INHERITS (%5);\n")
                .arg(tableName)
                .arg(QString(startDate.addMonths(mounthNumber).toString("MM")+startDate.addMonths(mounthNumber).toString("yy")))
                .arg(startDate.addMonths(mounthNumber).toString(Qt::ISODate))
                .arg(startDate.addMonths(mounthNumber+1).toString(Qt::ISODate))
                .arg(tableName);

        dateTables.append(createDateTableText);


        if(mounthNumber==0)
        {
            conditionName="IF";
        }
        else
        {
            conditionName="ELSIF";
        }


        // заполняем тело триггера для таблиц второго уровня (по датам)

        QString dateTriggerString=QString("%1 ( NEW.datetime >= DATE '%2' AND NEW.datetime < DATE '%3') THEN\n"
                                          "INSERT INTO %4 VALUES (NEW.*);\n")
                .arg(conditionName)
                .arg(startDate.addMonths(mounthNumber).toString(Qt::ISODate))
                .arg(startDate.addMonths(mounthNumber+1).toString(Qt::ISODate))
                .arg(tableName+"_"+QString(startDate.addMonths(mounthNumber).toString("MM")+startDate.addMonths(mounthNumber).toString("yy")));

        dateTablesTriggers.append(dateTriggerString);


        // заполняем конец триггера таблиц второго уровня
        if(mounthNumber==countOfMounth-1)
        {
            QString endOfTrigger=QString("ELSE\n"
                                         "RAISE EXCEPTION\n"
                                         "'There is no table to insert data with this date interval';\n"
                                         "END IF;\n"
                                         "RETURN NULL;\n"
                                         " END;\n"
                                         "$$\n"
                                         "LANGUAGE plpgsql;\n\n");
            dateTablesTriggers.append(endOfTrigger);

            QString usingTrigger=QString("CREATE TRIGGER %1_insert_trigger\n"
                                         "BEFORE INSERT ON %1\n"
                                         "FOR EACH ROW EXECUTE PROCEDURE %1_insert();\n\n\n").arg(tableName);

            dateTablesTriggers.append(usingTrigger);
        }


    }
    QFile dateFile("dateTables.txt");

    if (!dateFile.open(QFile::WriteOnly | QFile::Text | QFile::Truncate))
    {
        cout<<"Не удалось открыть файл"<<endl;
        return;
    }
    QTextStream dateStream(&dateFile);
    dateStream<<dateTables<<dateTablesTriggers;
    dateFile.close();
}

int main()
{
    QTextStream cout(stdout);
    QTextStream in (stdin);


    QTextCodec::setCodecForLocale(QTextCodec::codecForName("IBM 866"));

    cout <<"Enter the table name:"<< endl;

    QString tableName;
    in>> tableName;

    cout <<"Enter the start date:"<< endl;
    QString date;
    in >>date;
    QDate startDate= QDate::fromString(date,Qt::ISODate);

    qint32 countOfMounth=12;

    if(tableName=="dtmi" || tableName=="orient")
    {
        cout <<"Enter device numbers (separeted by commas):"<< endl;

        QString deviceNumbersStr;
        in>>deviceNumbersStr;
        QStringList deviceNumbers=deviceNumbersStr.split(',');

        createTwoLevelTables(tableName,deviceNumbers,countOfMounth,startDate);
    }
    else
    {
        createOneLevelTable(tableName,countOfMounth,startDate);
    }
    std::cin.get();
    return 0;
}
