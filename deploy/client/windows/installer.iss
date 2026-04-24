#define MyAppName "Legato Client"
#define MyAppPublisher "Legato"
#define MyAppExeName "legatofs.exe"
#define MyAppVersion GetEnv("LEGATO_VERSION")
#define MyAppSourceDir GetEnv("LEGATO_SOURCE_DIR")
#define MyAppOutputDir GetEnv("LEGATO_OUTPUT_DIR")

[Setup]
AppId={{0C5F2E1C-8E0A-4D9D-91F9-857961AC1A1A}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={autopf}\Legato
DefaultGroupName=Legato
UninstallDisplayIcon={app}\{#MyAppExeName}
ArchitecturesInstallIn64BitMode=x64compatible
Compression=lzma
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=admin
OutputDir={#MyAppOutputDir}
OutputBaseFilename=legatofs-{#MyAppVersion}-windows

[Files]
Source: "{#MyAppSourceDir}\legatofs.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#MyAppSourceDir}\register-client.ps1"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#MyAppSourceDir}\certs-README.txt"; DestDir: "{commonappdata}\Legato"; Flags: ignoreversion

[Dirs]
Name: "{commonappdata}\Legato"
Name: "{commonappdata}\Legato\certs"
Name: "{commonappdata}\Legato\catalog"
Name: "{commonappdata}\Legato\segments"
Name: "{commonappdata}\Legato\checkpoints"

[Icons]
Name: "{group}\Legato Config"; Filename: "{commonappdata}\Legato\legatofs.toml"
Name: "{group}\Register Legato Client"; Filename: "powershell.exe"; Parameters: "-ExecutionPolicy Bypass -File ""{app}\register-client.ps1"""
Name: "{group}\Uninstall Legato Client"; Filename: "{uninstallexe}"

[Code]
var
  BootstrapPage: TInputQueryWizardPage;
  ClientNamePage: TInputQueryWizardPage;
  MountPointPage: TInputQueryWizardPage;

procedure InitializeWizard;
begin
  BootstrapPage := CreateInputQueryPage(
    wpSelectDir,
    'Legato Server',
    'Discover or connect to the Legato server',
    'Leave the bootstrap URL blank to discover the Legato server on your LAN, or enter the server bootstrap URL if discovery is blocked.'
  );
  BootstrapPage.Add('Bootstrap URL:', False);
  BootstrapPage.Values[0] := '';

  ClientNamePage := CreateInputQueryPage(
    BootstrapPage.ID,
    'Client Name',
    'Name this client',
    'This name is embedded into the client certificate issued by the server.'
  );
  ClientNamePage.Add('Client name:', False);
  ClientNamePage.Values[0] := GetComputerNameString;

  MountPointPage := CreateInputQueryPage(
    ClientNamePage.ID,
    'Mount Point',
    'Choose the Legato mount point',
    'Enter the Windows mount point the client should expose.'
  );
  MountPointPage.Add('Mount point:', False);
  MountPointPage.Values[0] := 'L:\Legato';
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  BootstrapUrl: string;
  InstallArgs: string;
  ResultCode: Integer;
begin
  if CurStep = ssPostInstall then
  begin
    BootstrapUrl := Trim(BootstrapPage.Values[0]);

    InstallArgs :=
      'install ' +
      '--client-name "' + ClientNamePage.Values[0] + '" ' +
      '--mount-point "' + MountPointPage.Values[0] + '" ' +
      '--state-dir "' + ExpandConstant('{commonappdata}\Legato') + '" ' +
      '--library-root "/" ' +
      '--force';
    if BootstrapUrl <> '' then
    begin
      InstallArgs := InstallArgs + ' --bootstrap-url "' + BootstrapUrl + '"';
    end;
    if not Exec(ExpandConstant('{app}\legatofs.exe'), InstallArgs, '', SW_HIDE, ewWaitUntilTerminated, ResultCode) then
    begin
      RaiseException('Failed to run Legato client setup.');
    end;
    if ResultCode <> 0 then
    begin
      RaiseException('Legato client setup failed. Confirm the server bootstrap endpoint is reachable, then rerun this installer.');
    end;

    if not Exec(ExpandConstant('{app}\legatofs.exe'), 'service install --force', '', SW_HIDE, ewWaitUntilTerminated, ResultCode) then
    begin
      RaiseException('Failed to install the Legato scheduled task.');
    end;
    if ResultCode <> 0 then
    begin
      RaiseException('Legato scheduled task installation failed.');
    end;

    if not Exec(ExpandConstant('{app}\legatofs.exe'), 'service start', '', SW_HIDE, ewWaitUntilTerminated, ResultCode) then
    begin
      RaiseException('Failed to start the Legato scheduled task.');
    end;
    if ResultCode <> 0 then
    begin
      RaiseException('Legato scheduled task start failed.');
    end;
  end;
end;
