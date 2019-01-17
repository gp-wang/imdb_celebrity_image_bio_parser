#!/usr/bin/env python2
#
# Copyright 2015-2016 Carnegie Mellon University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import utils
import argparse
import cv2
import numpy as np
import os
import random
import shutil



# OPENFACE_ROOT = os.path.dirname(os.path.realpath(__file__))
# gw: path to openface root
OPENFACE_ROOT = "/mnt/sshfs/c4-openface"
modelDir = os.path.join(OPENFACE_ROOT, 'models')
dlibModelDir = os.path.join(modelDir, 'dlib')
openfaceModelDir = os.path.join(modelDir, 'openface')


# https://stackoverflow.com/questions/67631/how-to-import-a-module-given-the-full-path
import sys
sys.path.append(OPENFACE_ROOT)

import openface

IN_IMAGE_ROOT = "align_input"           # TODO
OUT_ALIGN_ROOT="align_output"
NEW_SIZE =160

def align_image_record(image_record):
    landmarkIndices = openface.AlignDlib.OUTER_EYES_AND_NOSE
    dlib_face_predictor_fpath = os.path.join(dlibModelDir, "shape_predictor_68_face_landmarks.dat")
    align = openface.AlignDlib( dlib_face_predictor_fpath )

    in_image_fpath = os.path.join(IN_IMAGE_ROOT, image_record.ms1m_id, image_record.fname)
    
    bgr = cv2.imread(in_image_fpath)
    rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)

    outRgb = align.align(NEW_SIZE, rgb,
                         landmarkIndices=landmarkIndices,
                         skipMulti=False)

    if outRgb is not None:
        outBgr = cv2.cvtColor(outRgb, cv2.COLOR_RGB2BGR)
        out_image_dirname = os.path.join(OUT_ALIGN_ROOT, image_record.ms1m_id)

        if not os.path.isdir(out_image_dirname):
            os.makedirs(out_image_dirname)
            
        out_image_fpath = os.path.join(out_image_dirname, image_record.fname)
        cv2.imwrite(out_image_fpath, outBgr)

    
    

if __name__ ==  "__main__":
    # gw: put import here because we don't want to create circular import (if you put it on the top) when you use this script as a module
    from utils import ImageRecord

    image_record = ImageRecord("m.00000001", "Test User", "dummy_url", "dummy.jpg")

    align_image_record(image_record)
    print("done align")

    
    
