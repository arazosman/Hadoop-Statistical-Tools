#ifndef MAINWINDOW_H
#define MAINWINDOW_H

#include <QMainWindow>
#include <QProcess>
#include <QUrl>

QT_BEGIN_NAMESPACE
namespace Ui { class MainWindow; }
QT_END_NAMESPACE

class MainWindow : public QMainWindow
{
    Q_OBJECT

public:
    MainWindow(QWidget *parent = nullptr);
    ~MainWindow();

    void initHadoop();
    void processCommand(QString command, QStringList args);
    QStringList generateCommandLineArguments();
    QString getStatisticalFunction();

private slots:
    void on_buttonHadoopPath_clicked();
    void on_buttonMapRedPath_clicked();
    void on_buttonDatasetPath_clicked();
    void on_buttonStart_clicked();

private:
    Ui::MainWindow *ui;
    QString hadoopPath;
    QString mapredPath;
    QString datasetPath;
    QProcess *terminalProcess;
    QString terminalOutput;
    bool firstTime = true;
};

#endif // MAINWINDOW_H
