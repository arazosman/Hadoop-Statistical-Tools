#include "mainwindow.h"
#include "ui_mainwindow.h"

#include <QFileDialog>
#include <QProcess>
#include <QDebug>
#include <QStyle>

MainWindow::MainWindow(QWidget *parent) : QMainWindow(parent), ui(new Ui::MainWindow)
{
    ui->setupUi(this);
    setTempDir();
    terminalProcess = new QProcess(this);
}

MainWindow::~MainWindow()
{
    delete ui;
}

void MainWindow::on_buttonHadoopPath_clicked()
{
    hadoopPath = QFileDialog::getExistingDirectory(this, tr("Select Folder of Hadoop Binary File"), "/home",
                                                   QFileDialog::ShowDirsOnly | QFileDialog::DontResolveSymlinks);
    ui->buttonHadoopPath->setText(QFile(hadoopPath).fileName());
}

void MainWindow::on_buttonDatasetPath_clicked()
{
    datasetPath = QFileDialog::getOpenFileName(this, tr("Select Dataset File"));
    ui->buttonDatasetPath->setText(QFile(datasetPath).fileName());
}

void MainWindow::on_buttonStart_clicked()
{
    if (firstTime)
    {
        initHadoop();
        firstTime = false;
    }

    terminalOutput.clear();

    processCommand("printf", {"###### STARTING MAP-REDUCE TASKS ######\n"});
    processCommand(hadoopPath + "/bin/hadoop", generateCommandLineArguments());
    processCommand("printf", {"\n###### PROCESSES COMPLETED. RESULTS: ######\n\n"});
    processCommand(hadoopPath + "/bin/hdfs", {"dfs", "-cat", "output/*"});
}

void MainWindow::initHadoop()
{
    processCommand("printf", {"###### STOPPING CURRENT HADOOP SERVICES ######\n\n"});
    processCommand(hadoopPath + "/sbin/stop-yarn.sh", {});
    processCommand(hadoopPath + "/sbin/stop-dfs.sh", {});
    processCommand("printf", {"\n###### FORMATTING HADOOP NAMENODE ######\n\n"});
    processCommand(hadoopPath + "/bin/hdfs", {"namenode", "-format", "-force"});

    terminalOutput.clear();

    processCommand("printf", {"###### STARTING NEW HADOOP SERVICES ######\n\n"});
    processCommand(hadoopPath + "/sbin/start-dfs.sh", {});
    processCommand(hadoopPath + "/sbin/start-yarn.sh", {});
    processCommand("printf", {"\n###### INITIALIZING HADOOP FILE SYSTEM ######\n"});
    processCommand(hadoopPath + "/bin/hdfs", {"dfs", "-mkdir", "-p", "/user/hadoop"});
    processCommand(hadoopPath + "/bin/hdfs", {"dfs", "-mkdir", "input"});
    processCommand("printf", {"\n###### PUTTING DATASET INTO HADOOP FILE SYSTEM ######\n"});
    processCommand(hadoopPath + "/bin/hdfs", {"dfs", "-put", datasetPath, "input"});
}

void MainWindow::processCommand(QString command, QStringList args)
{
    terminalProcess->start(command, args);
    terminalProcess->waitForFinished();
    terminalProcess->terminate();
    terminalOutput.append(terminalProcess->readAll());
    ui->terminal->setText(terminalOutput);
    this->repaint();
}

void MainWindow::setTempDir()
{
    if (tempDir.isValid())
    {
        tempDir.autoRemove();
        mapredPath = tempDir.path() + "/MapReduce.jar";
        QFile::copy(":/Files/MapReduce.jar", mapredPath);
    }
}

QStringList MainWindow::generateCommandLineArguments()
{
    QString targetColumn = ui->textTargetColumn->text().replace(" ", "");
    QString dependentColumns = ui->textDependentColumns->text().replace(" ", "");

    if (dependentColumns.size() == 0)
        dependentColumns = "-1";

    QString statisticalFunc = getStatisticalFunction();

    return {"jar", mapredPath, "input", "output", targetColumn, dependentColumns, statisticalFunc};
}

QString MainWindow::getStatisticalFunction()
{
    if (ui->rb_sum->isChecked())
        return "sum";
    else if (ui->rb_min->isChecked())
        return "min";
    else if (ui->rb_max->isChecked())
        return "max";
    else if (ui->rb_avg->isChecked())
        return "avg";
    else if (ui->rb_med->isChecked())
        return "med";
    else if (ui->rb_mod->isChecked())
        return "mod";
    else if (ui->rb_cnt->isChecked())
        return "cnt";
    else if (ui->rb_var->isChecked())
        return "var";

    return "std";
}
