# JDBC Drivers Directory

## 📦 Required Files

Place Oracle JDBC driver here:
- **File name**: `ojdbc8.jar` (or `ojdbc11.jar` for newer versions)
- **This file is REQUIRED** for building Docker image

## 📥 Download

1. Visit Oracle JDBC Downloads:
   https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html

2. Select version matching your Oracle Database:
   - Oracle 12c+ → Use `ojdbc8.jar`
   - Oracle 19c+ → Use `ojdbc8.jar` or `ojdbc11.jar`
   - Oracle 21c+ → Use `ojdbc11.jar`

3. Accept Oracle License Agreement

4. Download and place file here:
   ```
   jars/
   └── ojdbc8.jar  ← Put file here
   ```

## ✅ Verify

After downloading, verify the file exists:

```powershell
# Windows
ls jars\ojdbc8.jar

# Linux/Mac
ls -lh jars/ojdbc8.jar
```

You should see a file around 4-5 MB in size.

## 🔒 Important Notes

- ⚠️ **DO NOT commit this file to git** - Oracle JDBC drivers are licensed software
- The `.gitignore` file is already configured to exclude `jars/ojdbc*.jar`
- Each developer/CI system must download this file independently
- Alternative: Use a private artifact repository (Nexus/Artifactory) if available

## 🐳 Docker Build

Once the JAR is in place, build your image:

```powershell
.\build_and_push.ps1
```

The Dockerfile will automatically copy this JAR to `/opt/spark/jars/` in the image.


