import re

import pandas as pd
from openai import OpenAI

client = OpenAI()
model = "gpt-4"
model = 'gpt-4-1106-preview'


# # work example
# completion = client.chat.completions.create(
#     model="gpt-3.5-turbo",
#     messages=[
#         {"role": "system",
#          "content": "You are a poetic assistant, skilled in explaining complex programming concepts with creative flair."},
#         {"role": "user", "content": "Compose a poem that explains the concept of recursion in programming."}
#     ]
# )
# print(completion.choices[0].message)


def stock_experience_report_summarizer(tmp_target_data: pd.Series):
    # tmp_target_data = target_data.loc[74, :]
    tmp_target_data = tmp_target_data.copy()

    tmp_target_data['url']
    content = tmp_target_data['content']

    system_content = """You are a stock expert. 
    You will receive a article which might be the personal investment experience article. 
    What you need to do is to read article and answer some question as specified format.
    Questions as below:
    Question 1 (Please answer with 'Y' or 'N'): Is this article is stock annual experience report?
    Question 2 (Please answer with 'Y' or 'N'): Is his investment profitable?
    Question 3 (Please answer as a percentage): Annual rate of return?
    Note: If the Question 1 answer is 'N',Question2 and 3 please also return 'N'.
    Because I will use python to get you answer, please must return according to the following format:
    [ANS1,ANS2,ANS3]

    Answer Example 1: ['Y','Y','50%']
    Answer Example 2: ['Y','N','-30%']
    Answer Example 3: ['N','N','N']"""

    article = f"""文章如下 : "{content}
    "
    文章結束，請依規定的格式回答
    """
    completion = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_content},
            {"role": "user", "content": article}
        ],
        n=3
    )
    tmp_target_data['completion_tokens'] = completion.usage.completion_tokens
    tmp_target_data['prompt_tokens'] = completion.usage.prompt_tokens
    tmp_target_data['total_tokens'] = completion.usage.total_tokens
    tmp_target_data['model'] = completion.model
    all_content_list = []
    for choice in completion.choices:
        all_content_list.append(choice.message.content)
    tmp_target_data['all_content'] = str(all_content_list)
    # start process respond
    respond_content = completion.choices[0].message.content
    tmp_target_data['message'] = respond_content
    try:
        respond_list = eval(respond_content)
        if not isinstance(respond_list, list) or len(respond_list) != 3:
            respond_list = ['N', 'N', 'N']
    except BaseException:
        respond_list = ['N', 'N', 'N']

    #
    tmp_target_data['is_report'] = str(respond_list[0])
    tmp_target_data['is_profitable'] = str(respond_list[1])
    ori_annual_rate = str(respond_list[2])
    tmp_target_data['ori_annual_rate'] = ori_annual_rate
    # tmp_target_data['annual_rate'] = ''.join(re.findall(r'\d+', ori_annual_rate))
    tmp_target_data['annual_rate'] = ''.join(re.findall(r'-?\d+\.?\d*', ori_annual_rate))

    return tmp_target_data
