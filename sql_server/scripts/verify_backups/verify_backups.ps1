Clear-Host

# Set variables
$testDatabaseName = "RestoreTestDB"
$logTable = "RestoreTestLog"
$databasesToRestore = @() # Override list of databases to test
$skipDatabases = @('A', 'B', 'C','D')

# Mount backup folder with hard-coded credentials
$BackupFolders = @("\\", "C:\Backup\SQLServer1")
$SqlServerInstances = @("MSSQL1","MSSQL2")
$UserName = ""
$Password = ConvertTo-SecureString "" -AsPlainText -Force
$Credential = New-Object System.Management.Automation.PSCredential ($UserName, $Password)
$cntDatabasesRestored = 0
$deleteRestoreTest = 0

foreach ($sqlserver in $SqlServerInstances){
    # Remove existing PSDrives if they exist
    foreach ($drive in (Get-PSDrive "U" -ErrorAction SilentlyContinue)) {
        Remove-PSDrive -Name $drive.Name -Force
    }

    New-PSDrive -Name "U" -PSProvider FileSystem -Root $BackupFolders[$SqlServerInstances.IndexOf($sqlserver)] -Credential $Credential

    # Get list of databases in the SQL Server instance
    if ($databasesToRestore.Count -gt 0) {
        $databases = $databasesToRestore
    } else {
        $databases = Invoke-Sqlcmd -Query "SELECT name FROM sys.databases WHERE name NOT IN ('master', 'model', 'msdb', 'tempdb') and state=0;" -ServerInstance $sqlserver 
    }

    write-host "databases: $databases"
    $index = [array]::IndexOf($SqlServerInstances, $sqlserver)
    # Loop through each database
    foreach ($database in $databases) {        
        if ($database.GetType().Name -eq "DataRow"){            
            $databaseName = $database.name
        }else{
            $databaseName = $database
        }        

         # Skip restore if database is in the skipDatabases array
        if ($skipDatabases -contains $databaseName) {
            Write-Host "Skipping restore for database $databaseName"
            continue
        }        
        
        # Get backup folder for the database
        $databaseBackupFolder = Join-Path $BackupFolders[$index] $databaseName
                

        # Skip if backup folder does not exist
        if (-not (Test-Path $databaseBackupFolder)) {
            Write-Host "Backup folder not found for database $databaseName."
            continue
        } else {
                
            # Get latest full backup file for the database
            $backupFiles = Get-ChildItem $databaseBackupFolder | Where-Object {!$_.PSIsContainer} | Sort-Object LastWriteTime -Descending
            #Write-Host "backup files" + $backupFiles
            $fullBackupFile = $backupFiles | Where-Object {$_.Name -like "*.bak"} | Select-Object -First 1
            
        
            # Skip if no full backup file found
            if ($fullBackupFile.FullName -eq $null) {
                Write-Host "No full backup file found for database $databaseName."
                continue
            } else {
                
            
                # Connect to SQL Server instance
                $sqlConnection = New-Object System.Data.SqlClient.SqlConnection
                $sqlConnection.ConnectionString = "Server=$env:COMPUTERNAME;Database=master;Integrated Security=True"                
                $sqlConnection.Open()

               # Get the logical file names from the backup
                # Get logical file names from backup
                $fileList = Invoke-Sqlcmd -ServerInstance $serverInstance -Database 'master' -Query "RESTORE FILELISTONLY FROM DISK = '$databaseBackupFolder\$fullBackupFile'"
                $logicalNames = $fileList.LogicalName
                $logicalTypes = $fileList.Type

                $defaultPaths = Invoke-Sqlcmd -ServerInstance 'localhost' -Database 'master' -Query "SELECT SERVERPROPERTY('InstanceDefaultDataPath') AS InstanceDefaultDataPath,SERVERPROPERTY('InstanceDefaultLogPath') AS InstanceDefaultLogPath"                

               # Rename the logical file names to include the test database name
                $renamedLogicalNames = foreach ($name in $logicalNames) {
                    if ($logicalTypes[$logicalNames.IndexOf($name)] -eq 'D') {
                        "$testDatabaseName" + "_" + $name + ".mdf"
                    } else {
                        "$testDatabaseName" + "_" + $name + ".ldf"
                    }
                }

                Write-host "New File Names:"
                foreach ($file in $renamedLogicalNames) {
                Write-Host $file
                }

                write-host ""

                # Restore the backup to the test database with the renamed logical file names
                $restoreCommand = "RESTORE DATABASE [$testDatabaseName] FROM DISK = N'$($fullBackupFile.FullName)' WITH REPLACE,"
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
                        $sqlCommand.ExecuteNonQuery()                                        
                    }

                # Log test result in table
                Write-Host "Restored in: $timeTaken.TotalSeconds seconds"
                $logCommand = "INSERT INTO [$logTable] (DatabaseName, BackupFile, TestDate, TestResult) VALUES ('$databaseName', '$($fullBackupFile.Name)', GETDATE(), 'Success')"
                
                Write-host "Databases restored so far: $cntDatabasesRestored"
                
                } catch {

                    $logCommand = "INSERT INTO [$logTable] (DatabaseName, BackupFile, TestDate, TestResult, ErrorMessage) VALUES ('$databaseName', '$($fullBackupFile.Name)', GETDATE(), 'Failed', '$($_.Exception.Message)')"                    
                }    
                $cntDatabasesRestored += 1  
                $sqlCommand = New-Object System.Data.SqlClient.SqlCommand($logCommand, $sqlConnection)          
                $sqlCommand.ExecuteNonQuery()
                
                

                # Close SQL Server connection
                $sqlConnection.Close()                                

            }        
        
            # Output test information
            Write-Host "Database: $databaseName"
            Write-Host "Backup file: $($fullBackupFile.Name)"
            Write-Host "Backup folder: $databaseBackupFolder"
        }           
    }    
    if ( $cntDatabasesRestored -eq $databasesToRestore.Count -and $databasesToRestore.Count -gt 0) { break }             
}

# Connect to SQL Server instance
$sqlConnection = New-Object System.Data.SqlClient.SqlConnection
$sqlConnection.ConnectionString = "Server=$env:COMPUTERNAME;Database=master;Integrated Security=True"                
$sqlConnection.Open()

if ($deleteRestoreTest) {
# Drop test database
$dropCommand = "USE [master]; DROP DATABASE [$testDatabaseName];"
$sqlCommand = New-Object System.Data.SqlClient.SqlCommand($dropCommand, $sqlConnection)
$sqlCommand.ExecuteNonQuery()
}

# Close SQL Server connection
$sqlConnection.Close()       