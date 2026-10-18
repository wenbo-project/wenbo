@ECHO OFF

pushd %~dp0

if "%SPHINXBUILD%" == "" (
    set SPHINXBUILD=python -m sphinx
)
set SOURCEDIR=.
set BUILDDIR=../.local/docs-build

if "%1" == "" goto help

%SPHINXBUILD% -M %1 "%SOURCEDIR%" "%BUILDDIR%" -W --keep-going %SPHINXOPTS% %O%
set BUILDSTATUS=%ERRORLEVEL%
goto end

:help
%SPHINXBUILD% -M help "%SOURCEDIR%" "%BUILDDIR%" %SPHINXOPTS% %O%

set BUILDSTATUS=%ERRORLEVEL%

:end
popd
exit /b %BUILDSTATUS%
