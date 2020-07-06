import pathlib
import pandas as pd
import geopandas as gpd
import datetime

def append_wetnessIndex(df: pd.DataFrame, pathWetnessIndex: pathlib.Path):
    # Read and clean
    wetnessIndex = (pd.read_excel(
        pathWetnessIndex,
        sheet_name="Sheet1",
        usecols=["ID2", "Ele", "TWI", "RSP", "AGSR"])
        .rename(
            columns={
                "Ele": "Elevation", 
                "TWI": "TopographicWetnessIndex", 
                "RSP": "RelativeSlopePosition",
                "AGSR": "AnnualGlobalSolarRadiation"
            }
    ))

    # Merge by ID2
    result_df = df.copy()
    result_df = result_df.merge(wetnessIndex, on = "ID2", how = "left")

    return(result_df)

def append_rootingDepth(df: pd.DataFrame, pathRootingDepth: pathlib.Path):
    # Read and clean data
    rootingDepth = (pd.read_excel(pathRootingDepth, sheet_name="Sheet1", usecols=[0,1,2])
        .query("StudyArea == 'CE'")
        .assign(ID2 = lambda x: pd.to_numeric(x["ID"], downcast="integer"))
        .rename(columns={"Few Roots, cm":"RootDepth"})
        .drop(["StudyArea", "ID"], axis = 1))

    # Merge by ID2
    result_df = df.copy()
    result_df = result_df.merge(rootingDepth, on = "ID2", how = "left")

    return result_df

def append_relativeYieldCV(df: pd.DataFrame, pathRelativeYield: pathlib.Path):
    relativeYield = (pd.read_csv(
        pathRelativeYield,
        usecols=["HarvestYear", "ID2", "RelativeYield"]))
    
    relativeYieldMean = (relativeYield.groupby("ID2")["RelativeYield"].mean()).rename("RelativeYieldMean")
    relativeYieldCV = (relativeYield.groupby("ID2")["RelativeYield"].std() / 
        relativeYield.groupby("ID2")["RelativeYield"].mean()).rename("RelativeYieldCV")

    result_df = df.copy()
    result_df = (result_df
        .merge(relativeYieldCV, on = "ID2", how = "left")
        .merge(relativeYieldMean, on = "ID2", how = "left"))

    return result_df



def main(
    pathWetnessIndex: pathlib.Path,
    pathRootingDepth: pathlib.Path, 
    pathGeorefPoints: pathlib.Path, 
    pathRelativeYield: pathlib.Path,
    workingDir: pathlib.Path
):
    ### Data Preparation
    georefPointsIn = gpd.read_file(pathGeorefPoints)
    gp = georefPointsIn.assign(
        Latitude = georefPointsIn.geometry.y,
        Longitude = georefPointsIn.geometry.x).sort_values(by="ID2")
    
    df = (gp
        .pipe(append_wetnessIndex, pathWetnessIndex)
        .pipe(append_rootingDepth, pathRootingDepth)
        .pipe(append_relativeYieldCV, pathRelativeYield)
        .drop(["geometry"], axis = 1))  

    # Write file
    workingDir.mkdir(parents=True, exist_ok=True)
    date_today = datetime.datetime.now().strftime("%Y%m%d")

    df.to_csv(
        workingDir / "cleaned_data_{}_P2A1.csv".format(date_today),
        index = False)


if __name__ == "__main__":
    # parameters
    inputPathWetnessIndex = pathlib.Path.cwd() / "input" / "Final terrain attributes for each georeference points from SAGA_clean version for R_ 06122019.xlsx"
    inputPathRootingDepth = pathlib.Path.cwd() / "input" / "Topsoil yield.xlsx"
    inputPathGeorefPoints = pathlib.Path.cwd() / "input" / "cookeast_georeferencepoint_20190924.geojson"
    inputPathRelativeYield = pathlib.Path.cwd() / "input" / "relativeYield_1999-2015_20200605_P3A1.csv"
    workingDir = pathlib.Path.cwd() / "working"

    main(
        inputPathWetnessIndex, 
        inputPathRootingDepth,
        inputPathGeorefPoints,
        inputPathRelativeYield,
        workingDir)
