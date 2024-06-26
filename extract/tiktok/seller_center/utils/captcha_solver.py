import cv2
import numpy as np
from sewar.full_ref import ssim


def extract_coor(x, y, w, h):
    x1, y1 = x, y
    x2, y2 = x + w, y
    x3, y3 = x, y + h
    x4, y4 = x + w, y + h

    return [(x1, y1), (x2, y2), (x3, y3), (x4, y4)]


def clear_bbox(bboxes):
    flags = [True for i in bboxes]

    for i in range(len(bboxes)):
        x_A, y_A, w_A, h_A = bboxes[i]

        if flags[i] == False:
            continue

        for j in range(len(bboxes)):
            if i == j:
                continue

            x_B, y_B, w_B, h_B = bboxes[j]

            coors_A = extract_coor(x_A, y_A, w_A, h_A)
            coors_B = extract_coor(x_B, y_B, w_B, h_B)

            flag_coor1 = (coors_B[0][0] <= coors_A[0][0]) and (coors_B[0][1] <= coors_A[0][1])
            flag_coor2 = (coors_B[1][0] >= coors_A[1][0]) and (coors_B[1][1] <= coors_A[1][1])
            flag_coor3 = (coors_B[2][0] <= coors_A[2][0]) and (coors_B[2][1] >= coors_A[2][1])
            flag_coor4 = (coors_B[3][0] >= coors_A[3][0]) and (coors_B[3][1] >= coors_A[3][1])

            if flag_coor1 and flag_coor2 and flag_coor3 and flag_coor4:
                flags[j] = False

    clean_bboxes = [bboxes[i] for i in range(len(bboxes)) if flags[i] == True]

    return clean_bboxes


def detect(image, mser):
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    regions, boundingBoxes = mser.detectRegions(gray)
    boundingBoxes = clear_bbox(boundingBoxes)

    return boundingBoxes


def same_coors(bboxes, index):
    x1, y1, w1, h1 = bboxes[index[0]]
    x2, y2, w2, h2 = bboxes[index[1]]

    coor1 = (x1 + w1 // 2, y1 + h1 // 2)
    coor2 = (x2 + w2 // 2, y2 + h2 // 2)

    return coor1, coor2


def localize(boundingBoxes, image):
    objs = []
    for (x, y, w, h) in boundingBoxes:
        objs.append(cv2.cvtColor(image[y:y + h, x:x + w], cv2.COLOR_BGR2GRAY))

    return objs


def get_scores(objs):
    sim_scores = []

    for i in range(len(objs)):
        sim_score = []
        obj_A = objs[i]

        for j in range(len(objs)):
            if i == j:
                sim_score.append(-1)
                continue

            obj_B = objs[j]

            is_bigger = obj_A.shape > obj_B.shape

            newW = obj_A.shape[1] if is_bigger else obj_B.shape[1]
            newH = obj_A.shape[0] if is_bigger else obj_B.shape[0]

            obj_A = cv2.resize(obj_A, (newW, newH), interpolation=cv2.INTER_AREA)
            obj_B = cv2.resize(obj_B, (newW, newH), interpolation=cv2.INTER_AREA)

            sim_score.append(ssim(obj_A, obj_B)[0])

        sim_scores.append(sim_score)

    return sim_scores


def execute(filename):
    # Score threshold
    threshold = 0.7

    mser = cv2.MSER_create(delta=4, min_area=300, max_area=4500, max_variation=0.35, min_diversity=0.1)

    # Load file
    image = cv2.imread(filename)

    # Detect bounding boxes
    boundingBoxes = detect(image, mser)

    # Extract objects and calculate similarity score
    objs = localize(boundingBoxes, image)
    sim_scores = np.array(get_scores(objs))

    # Grab index of objects with the highest similarity score
    index = np.unravel_index(sim_scores.argmax(), sim_scores.shape)

    if sim_scores[index[0], index[1]] < threshold:
        # If similarity score does not fulfill the threshold
        # Skip/Refresh the captcha
        return 'Refresh the CAPTCHA!', 'Refresh the CAPTCHA!'
    else:
        # Extract coordinates of the pinpoint
        coor1, coor2 = same_coors(boundingBoxes, index)
        return coor1, coor2
