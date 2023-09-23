Clear-Host


#read config.json from current script path
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Definition
$config = Get-Content "$scriptPath\config.json" | Out-String | ConvertFrom-Json

$logTable=$config.log_table

# SQL query to create log_table table if it doesn't exist
$createTableSql = @"
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '$logTable')
BEGIN
    CREATE TABLE $logTable (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        DatabaseName NVARCHAR(255),
        BackupFile NVARCHAR(255),
        TestDate DATETIME,
        TestResult NVARCHAR(50),
        ErrorMessage NVARCHAR(MAX)
    );
END
"@


foreach ($sqlserver in $config.instances) {
    write-host "Starting to process sqlserver: " $sqlserver.name
    $databasesToRestore=$null
    $databasesOnInstance=$null

    #check if $sqlserver.backups.backup_path is network share
    <#
    if ($sqlserver.backups.backup_path -like "\\*"){

         # Remove existing PSDrives if they exist
        foreach ($drive in (Get-PSDrive "U" -ErrorAction SilentlyContinue)) {
            Remove-PSDrive -Name $drive.Name -Force
        }    

         #check if node $sqlserver.backups.backup_auth exists in $config then create $Credential and create PSDrive          
         
         
        if ($null -ne $sqlserver.backups.backup_auth){
            $UserName = $sqlserver.backups.backup_auth.username
            $Password = ConvertTo-SecureString $sqlserver.backups.backup_auth.password -AsPlainText -Force
            $Credential = New-Object System.Management.Automation.PSCredential ($UserName, $Password)      

            New-PSDrive -Name "U" -PSProvider FileSystem -Root $sqlserver.backups.backup_path -Credential $Credential

        }else{
            ##create PSDrive without credentials
            #not tested
            New-PSDrive -Name "U" -PSProvider FileSystem -Root $sqlserver.backups.backup_path
        }  

        $BackupFolder += @("U:\")                 

    }else{
        $BackupFolder += @($sqlserver.backups.backup_path)
    }  
    #> 

    # Connect to SQL Server instance
    $databasesOnInstance = Invoke-Sqlcmd -Query "SELECT name FROM sys.databases WHERE name NOT IN ('master', 'model', 'msdb', 'tempdb') and state=0;" -ServerInstance $sqlserver.name 

    #print the list of databases found on  instance
    write-host ("databases found on " + $sqlserver.name + ": $($databasesOnInstance | Format-Table -AutoSize | Out-String)")

    #if instances.databases exists then use it to restore the databases on  instance else use the list of databases found on  instance to restore
    if ($null -ne $sqlserver.databases){
        write-host ("databases to restore on "  + $sqlserver.name + "}: $($sqlserver.databases | Format-Table -AutoSize | Out-String)")
        $databasesToRestore = $sqlserver.databases
    }else{
        $databasesToRestore = $databasesOnInstance.name
    }    
    
    #if instances.skip_databases exists remove the databases from $databasesToRestore
    if ($null -ne $sqlserver.skip_databases){
        foreach ($database in $sqlserver.skip_databases){
            $databasesToRestore = $databasesToRestore -notmatch $database
        }
        write-host ("databases to skip on " + $sqlserver.name + ": $($sqlserver.skip_databases | Format-Table -AutoSize | Out-String)")
    }

    #print the list of databases to restore
    write-host ("databases to restore on " +  $sqlserver.name + ": $($databasesToRestore | Format-Table -AutoSize | Out-String)")
    
     # Establish connection to restore server
     $sqlConnection = New-Object System.Data.SqlClient.SqlConnection    
     if ($config.restore_server.auth_type -eq "os"){
         $sqlConnection.ConnectionString = "Server="+$config.restore_server.host+";Database=master;Integrated Security=True"
     }else{
         $sqlConnection.ConnectionString = "Server="+$config.restore_server.host+";Database=master;User Id=${config.restore_server.auth.username};Password=${config.restore_server.auth.password}"
     }    
     $sqlConnection.Open()

     # Create log_table table if it doesn't exist
     $command = $sqlConnection.CreateCommand()
     $command.CommandText = $createTableSql     
     $output=$command.ExecuteNonQuery()


    #default paths of the restore server
    $defaultPaths = Invoke-Sqlcmd -ServerInstance $config.restore_server.host -Database 'master' -Query "SELECT SERVERPROPERTY('InstanceDefaultDataPath') AS InstanceDefaultDataPath,SERVERPROPERTY('InstanceDefaultLogPath') AS InstanceDefaultLogPath"                
    Write-Host "Default Paths of the restore server:" $defaultPaths.InstanceDefaultDataPath $defaultPaths.InstanceDefaultLogPath

    
    foreach ($database in $databasesToRestore){                  
        # Get backup folder for the database
        $databaseBackupFolder = Join-Path $sqlserver.backups.backup_path $database
        
        #check if backup folder exists
        if (-not (Test-Path $databaseBackupFolder)) {
            Write-Host "Backup folder not found for database $database."
            continue
        } 

        $backupFiles = Get-ChildItem $databaseBackupFolder | Where-Object {!$_.PSIsContainer} | Sort-Object LastWriteTime -Descending
        $fullBackupFile = $backupFiles | Where-Object {$_.Name -like "*.bak"} | Select-Object -First 1

        #check if full backup file exists
        if ($null -eq $fullBackupFile.FullName) {
            Write-Host "No full backup file found for database $database."
            continue
        }
        #write startign restore message with timestamp, server is restore_server from config.json
        write-host ("Starting restore of " + $database + " on " + $config.restore_server.host + " at " + (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))        
        Write-Host "Database filename:" + $fullBackupFile.FullName

       
        # Get the logical file names from the backup    
        $fileList = Invoke-Sqlcmd -ServerInstance $sqlserver.name -Database 'master' -Query "RESTORE FILELISTONLY FROM DISK = '$databaseBackupFolder\$fullBackupFile'"
        $logicalNames = $fileList.LogicalName
        $logicalTypes = $fileList.Type
        
        #print the logical file names 
        write-host ("logical file names for " + $database + ": $($logicalNames | Format-Table -AutoSize | Out-String)")
        
         # Rename the logical file names to include the test database name
         $renamedLogicalNames = foreach ($name in $logicalNames) {
            if ($logicalTypes[$logicalNames.IndexOf($name)] -eq 'D') {
                $config.dummy_db_name + "_" + $name + ".mdf"
            } else {
                $config.dummy_db_name + "_" + $name + ".ldf"
            }
        }

        Write-host "New File Names:"
        foreach ($file in $renamedLogicalNames) {
        Write-Host $file.
        }

        # Restore the backup to the test database with the renamed logical file names
        $restoreCommand = "RESTORE DATABASE ["+$config.dummy_db_name+"] FROM DISK = N'$($fullBackupFile.FullName)' WITH REPLACE,"
        $restoreCommand += "MOVE '$($logicalNames[0])' TO '" + $defaultPaths.InstanceDefaultDataPath + "$($renamedLogicalNames[0])',"

        for ($i = 1; $i -lt $logicalNames.Count; $i++) {
            $restoreCommand += "MOVE '$($logicalNames[$i])' TO '" + $defaultPaths.InstanceDefaultLogPath + "$($renamedLogicalNames[$i])',"
        }

        $restoreCommand = $restoreCommand.TrimEnd(",")
        Write-Host "restoreCommand: $restoreCommand"   
        
        $sqlCommand = New-Object System.Data.SqlClient.SqlCommand($restoreCommand, $sqlConnection)
        $sqlCommand.CommandTimeout = 1200

        try{                   
            $timeTaken = Measure-Command {
                #$sqlCommand.ExecuteNonQuery()                                        
            }

        # Log test result in table
        Write-Host "Restored in: $timeTaken.TotalSeconds seconds"
        $logCommand = "INSERT INTO $logTable (DatabaseName, BackupFile, TestDate, TestResult) VALUES ('$database', '$($fullBackupFile.Name)', GETDATE(), 'Success')"
        
        Write-host "Databases restored so far: $cntDatabasesRestored"
        
        } catch {

            $logCommand = "INSERT INTO $logTable (DatabaseName, BackupFile, TestDate, TestResult, ErrorMessage) VALUES ('$database', '$($fullBackupFile.Name)', GETDATE(), 'Failed', '$($_.Exception.Message)')"                    
        }    
        $cntDatabasesRestored += 1  
        $sqlCommand = New-Object System.Data.SqlClient.SqlCommand($logCommand, $sqlConnection)          
        $output=$sqlCommand.ExecuteNonQuery()
        
        write-host "--------------------------"

    }
}


# Drop the database on the restore server used for restore test
$dropCommand = "USE [master]; DROP DATABASE ["+$config.dummy_db_name+"];"
$sqlCommand = New-Object System.Data.SqlClient.SqlCommand($dropCommand, $sqlConnection)
$sqlCommand.ExecuteNonQuery()


$sqlConnection.Close()       
    
 
        
  
            
