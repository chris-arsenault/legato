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
  BundleDirPage: TInputQueryWizardPage;
  ServerEndpointPage: TInputQueryWizardPage;
  ServerNamePage: TInputQueryWizardPage;
  MountPointPage: TInputQueryWizardPage;

procedure InitializeWizard;
begin
  BundleDirPage := CreateInputQueryPage(
    wpSelectDir,
    'Client Bundle',
    'Optionally install a server-issued client bundle',
    'Enter the directory containing server-ca.pem, client.pem, and client-key.pem if you want the installer to register this client now.'
  );
  BundleDirPage.Add('Bundle directory:', False);
  BundleDirPage.Values[0] := '';

  ServerEndpointPage := CreateInputQueryPage(
    BundleDirPage.ID,
    'Legato Server',
    'Configure the Legato server endpoint',
    'Enter the Legato server endpoint the client should connect to.'
  );
  ServerEndpointPage.Add('Server endpoint:', False);
  ServerEndpointPage.Values[0] := 'legato.lan:7823';

  ServerNamePage := CreateInputQueryPage(
    ServerEndpointPage.ID,
    'TLS Server Name',
    'Configure the expected TLS server name',
    'Enter the DNS name expected in the Legato server certificate.'
  );
  ServerNamePage.Add('Server name:', False);
  ServerNamePage.Values[0] := 'legato.lan';

  MountPointPage := CreateInputQueryPage(
    ServerNamePage.ID,
    'Mount Point',
    'Configure the default Legato mount point',
    'Enter the Windows mount point the client should expose.'
  );
  MountPointPage.Add('Mount point:', False);
  MountPointPage.Values[0] := 'L:\Legato';
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  ConfigPath: string;
  ConfigContents: string;
  BundleDir: string;
  InstallArgs: string;
  ResultCode: Integer;
begin
  if CurStep = ssPostInstall then
  begin
    ConfigPath := ExpandConstant('{commonappdata}\Legato\legatofs.toml');
    BundleDir := Trim(BundleDirPage.Values[0]);

    if (BundleDir <> '') and DirExists(BundleDir) and
       FileExists(AddBackslash(BundleDir) + 'server-ca.pem') and
       FileExists(AddBackslash(BundleDir) + 'client.pem') and
       FileExists(AddBackslash(BundleDir) + 'client-key.pem') then
    begin
      InstallArgs :=
        'install ' +
        '--bundle-dir "' + BundleDir + '" ' +
        '--mount-point "' + MountPointPage.Values[0] + '" ' +
        '--state-dir "' + ExpandConstant('{commonappdata}\Legato') + '" ' +
        '--library-root "/srv/libraries" ' +
        '--force';
      if not Exec(ExpandConstant('{app}\legatofs.exe'), InstallArgs, '', SW_HIDE, ewWaitUntilTerminated, ResultCode) then
      begin
        RaiseException('Failed to execute legatofs install during Windows client registration.');
      end;
      if ResultCode <> 0 then
      begin
        RaiseException('legatofs install failed during Windows client registration.');
      end;
    end
    else if not FileExists(ConfigPath) then
    begin
      ConfigContents :=
        '[common.tracing]' + #13#10 +
        'json = false' + #13#10 +
        'level = "info"' + #13#10 + #13#10 +
        '[common.metrics]' + #13#10 +
        'prefix = "legatofs"' + #13#10 + #13#10 +
        '[client]' + #13#10 +
        'endpoint = "' + ServerEndpointPage.Values[0] + '"' + #13#10 + #13#10 +
        '[client.cache]' + #13#10 +
        'max_bytes = 1610612736000' + #13#10 + #13#10 +
        '[client.tls]' + #13#10 +
        'ca_cert_path = "C:\\ProgramData\\Legato\\certs\\server-ca.pem"' + #13#10 +
        'client_cert_path = "C:\\ProgramData\\Legato\\certs\\client.pem"' + #13#10 +
        'client_key_path = "C:\\ProgramData\\Legato\\certs\\client-key.pem"' + #13#10 +
        'server_name = "' + ServerNamePage.Values[0] + '"' + #13#10 + #13#10 +
        '[client.retry]' + #13#10 +
        'initial_delay_ms = 250' + #13#10 +
        'max_delay_ms = 5000' + #13#10 +
        'multiplier = 2' + #13#10 + #13#10 +
        '[mount]' + #13#10 +
        'mount_point = "' + MountPointPage.Values[0] + '"' + #13#10 +
        'library_root = "/srv/libraries"' + #13#10 +
        'state_dir = "C:\\ProgramData\\Legato"' + #13#10;
      SaveStringToFile(ConfigPath, ConfigContents, False);
    end;
  end;
end;
