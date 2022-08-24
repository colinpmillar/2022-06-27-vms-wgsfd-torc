
library(icesTAF)

# install analysis dependencies
install.deps()

# add data script (and also install its dependencies)
add.data.script(name = "logbook_static_gears")

# register script in DATA.bib
taf.roxygenise()

# fetch data
taf.bootstrap()
