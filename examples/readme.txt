Instructions to create a 0.5-degree Montage workflow for DEWE.v3

(1) In the Oregon region, launch an EC2 instance with AMI ami-f4e47cc4. SSH into the EC2 instance using username "montage".

(2) Run the following commands

mkdir Montage_0.5
cd Montaage_0.5
cp -r /opt/montage/v3.3/bin bin
mDAG 2mass j M17 0.5 0.5 0.0002777778 . file://$PWD file://$PWD/inputs
generate-montage-replica-catalog
mkdir workdir
mv *.tbl workdir
mv *.hdr workdir
cd workdir
mArchiveExec images.tbl
gunzip *.gz


Check replica.catalog, copy some files;

cp big_region.hdr big_region_20170514_055220_1565.hdr
cp pimages.tbl pimages_20170514_055220_1565.tbl
