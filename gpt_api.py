# openai.api_key = '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'

from openai import OpenAI

client = OpenAI()
model = "gpt-4"
model = 'gpt-4-1106-preview'
# work example
# completion = client.chat.completions.create(
#     model="gpt-3.5-turbo",
#     messages=[
#         {"role": "system",
#          "content": "You are a poetic assistant, skilled in explaining complex programming concepts with creative flair."},
#         {"role": "user", "content": "Compose a poem that explains the concept of recursion in programming."}
#     ]
# )






def stock_experience_report_summarizer(content):
    content = target_data['content'][0]

    system_content = """You are a stock expert. 
    You will receive a article which might be the personal investment experience article. 
    What you need to do is to read article and answer some question as specified format.
    Questions as below:
    Question 1 (Please answer with 'Y' or 'N'): Is this article is stock annual experience report?
    Question 2 (Please answer with 'Y' or 'N'): Is his investment is profitable?
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
        ]
    )

    print(completion.choices[0].message)
