#-*- coding:utf-8 -*-

from struct import pack
import torch
import torch.optim as optim
from tqdm import tqdm, tqdm_notebook
from kobert.utils import get_tokenizer
from kobert.pytorch_kobert import get_pytorch_kobert_model
from transformers import AdamW
from transformers.optimization import get_cosine_schedule_with_warmup
import argparse, os
from .models import *

p = argparse.ArgumentParser()

p.add_argument('--model_load_path', default='./classification/', type=str)
p.add_argument('--model_file', default='01_lp_classify_model.pt', type=str)

args = p.parse_args()

device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
print(device)
bertmodel, vocab = get_pytorch_kobert_model()

model = BERTClassifier(bertmodel,  dr_rate=0.5).to(device)

model_load_path=args.model_load_path
model_file_path=args.model_file
path=model_load_path+model_file_path
#path=args.model_file
print(path)
model.load_state_dict(torch.load(path, map_location=device), strict=False)
model.eval()


tokenizer = get_tokenizer()
tok = nlp.data.BERTSPTokenizer(tokenizer, vocab, lower=False)

def predict(predict_sentence):

    data = [predict_sentence, '0']
    dataset_another = [data]

    another_test = BERTDataset(dataset_another, 0, 1, tok, 128, True, False)
    test_dataloader = torch.utils.data.DataLoader(another_test, batch_size=5, num_workers=0)
    model.eval()
    
    for batch_id, (token_ids, valid_length, segment_ids, label) in enumerate(test_dataloader):
        token_ids = token_ids.long().to(device)
        segment_ids = segment_ids.long().to(device)

        valid_length= valid_length
        label = label.long().to(device)

        out = model(token_ids, valid_length, segment_ids)
              
        output_prob = torch.sigmoid(out)
        preds = torch.argmax(out,dim=1)
        
                
        test_eval=[]
        for i in out:
            logits=i
            logits = logits.detach().cpu().numpy()

            if np.argmax(logits) == 0:
                test_eval.append("가격")
            elif np.argmax(logits) == 1:
                test_eval.append("디자인")
            elif np.argmax(logits) == 2:
                test_eval.append("사용성")
            elif np.argmax(logits) == 3:
                test_eval.append("사이즈")
            elif np.argmax(logits) == 4:
                test_eval.append("재질")
            elif np.argmax(logits) == 5:
                test_eval.append("제품상태")
                      
        return test_eval[0]

'''
# data load
# path 수정
path='./data/'
#path='../empathy_finish_file_211107/'
file_list=os.listdir(path)

for i in file_list:
    csv_file_path=str(path+i)
    df=pd.read_csv(path+i)
    row_count=len(df.index)
    print(df)

    review=[]
    subject_list=[]

    for index in df.index:
        review=df.iloc[index,7]

        result=predict(review)
        print(result)
        subject_list.append(result)

    df['subject']=subject_list

    df.to_csv('./result/classify_'+i, index=False)
'''