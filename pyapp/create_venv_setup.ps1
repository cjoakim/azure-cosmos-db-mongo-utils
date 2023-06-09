# Recreate the python virtual environment and reinstall libs on Windows.
# Also create necessary output directories.
# Chris Joakim, Microsoft, 2023

$dirs = ".\venv\", ".\pyvenv.cfg"
foreach ($d in $dirs) {
    if (Test-Path $d) {
        Write-Output "deleting $d"
        Remove-Item $d -Force -Recurse
    } 
}

Write-Output 'creating new venv ...'
python -m venv .\venv\

Write-Output 'activating new python venv ...'
.\venv\Scripts\Activate.ps1
python --version

Write-Output 'upgrading pip ...'
python -m pip install --upgrade pip 

Write-Output 'install pip-tools ...'
pip install --upgrade pip-tools

Write-Output 'displaying python location and version'
python --version

Write-Output 'displaying pip location and version'
pip --version

Write-Output 'pip-compile requirements.in ...'
pip-compile --output-file .\requirements.txt .\requirements.in

Write-Output 'pip install requirements.txt ...'
pip install -q -r .\requirements.txt

Write-Output 'pip list ...'
pip list

# Create the following output directories that are git-ignored
$gitIgnoredDirectories = "artifacts/bicep","artifacts/spark","tmp","tmp/captured_docs"
foreach ($dir in $gitIgnoredDirectories)
{
    if (Test-Path $dir) {
        # the directory already exists
    }
    else
    {
        New-Item -ItemType Directory -Force -Path $dir
    }
}

Write-Output 'activating the venv...'
.\venv\Scripts\activate

python --version

# pip list output 2023/05/11 on Windows:
#
#Package         Version
#--------------- ---------
#arrow           0.17.0
#attrs           22.2.0
#build           0.10.0
#certifi         2020.12.5
#chardet         4.0.0
#click           8.1.3
#colorama        0.4.6
#dnspython       2.3.0
#docopt          0.6.2
#et-xmlfile      1.1.0
#flake8          3.8.4
#humanize        3.2.0
#idna            2.10
#iniconfig       2.0.0
#Jinja2          3.1.2
#MarkupSafe      2.1.2
#mccabe          0.6.1
#openpyxl        3.1.2
#packaging       23.0
#pip             23.1.2
#pip-tools       6.13.0
#pluggy          1.0.0
#psutil          5.8.0
#pycodestyle     2.6.0
#pyflakes        2.2.0
#pymongo         3.11.3
#pyproject_hooks 1.0.0
#pytest          7.2.2
#python-dateutil 2.8.1
#requests        2.25.1
#setuptools      65.5.0
#six             1.15.0
#urllib3         1.26.3
#wheel           0.40.0
