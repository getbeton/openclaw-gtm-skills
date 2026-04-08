#!/usr/bin/env python3
"""
Generate personalized email sequences for all campaign contacts.
Reads templates + contacts + vertical mapping, outputs a CSV ready for seqd.
"""

import json, csv, os, re
from datetime import datetime, timedelta

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Vertical -> user_type + data_action + churn examples + expansion signals
VERTICAL_MAP = {
    'logistics': {
        'user_type': 'fleet partners',
        'data_action': 'route, dispatch, or idle',
        'churn_a': 'a long-haul carrier slowing down is seasonal',
        'churn_b': 'a last-mile partner slowing down is a competitor poaching them',
        'expansion_signal': 'requesting new route types',
        'expansion_alt': 'increasing volume on existing routes',
    },
    'fintech': {
        'user_type': 'merchant accounts',
        'data_action': 'transact, onboard, or settle',
        'churn_a': 'a seasonal merchant dropping volume is normal',
        'churn_b': 'a steady merchant dropping volume is pre-churn',
        'expansion_signal': 'onboarding new payment methods',
        'expansion_alt': 'increasing transaction volume on existing rails',
    },
    'HR tech': {
        'user_type': 'talent on the platform',
        'data_action': 'apply, match, or engage',
        'churn_a': 'a specialist slowing their search is expected between placements',
        'churn_b': 'a generalist going quiet is them switching to a competitor',
        'expansion_signal': 'engaging with new job categories',
        'expansion_alt': 'increasing application volume in existing categories',
    },
    'LMS': {
        'user_type': 'enterprise learners',
        'data_action': 'complete courses, log in, or engage',
        'churn_a': 'a learner pausing during Q4 is seasonal',
        'churn_b': 'a team dropping usage mid-contract is a renewal risk',
        'expansion_signal': 'enrolling in new course tracks',
        'expansion_alt': 'increasing completion rates in existing tracks',
    },
    'telecom': {
        'user_type': 'business subscribers',
        'data_action': 'provision, consume, or expand services',
        'churn_a': 'an SMB reducing lines is seasonal headcount adjustment',
        'churn_b': 'an SMB reducing data usage is them testing a competitor',
        'expansion_signal': 'adding new service types',
        'expansion_alt': 'scaling existing service capacity',
    },
    'commerce': {
        'user_type': 'retail clients',
        'data_action': 'run campaigns, personalize, or segment',
        'churn_a': 'a retailer pausing campaigns during low season is normal',
        'churn_b': 'a retailer reducing personalization rules is them moving to another platform',
        'expansion_signal': 'launching new campaign types',
        'expansion_alt': 'increasing audience segments',
    },
    'cloud': {
        'user_type': 'hosted customers',
        'data_action': 'scale, deploy, or consume resources',
        'churn_a': 'a customer reducing instances during off-peak is expected',
        'churn_b': 'a customer migrating workloads elsewhere is pre-churn',
        'expansion_signal': 'provisioning new regions',
        'expansion_alt': 'scaling existing deployments',
    },
    'marketplace': {
        'user_type': 'buyers and sellers',
        'data_action': 'list, transact, or engage',
        'churn_a': 'a seller pausing listings is seasonal inventory adjustment',
        'churn_b': 'a seller reducing listings while competitors grow is churn',
        'expansion_signal': 'listing in new categories',
        'expansion_alt': 'increasing volume in existing categories',
    },
    'SaaS': {
        'user_type': 'active accounts',
        'data_action': 'log in, create, or integrate',
        'churn_a': 'a user going quiet during holidays is seasonal',
        'churn_b': 'a power user reducing API calls is them testing an alternative',
        'expansion_signal': 'activating new integrations',
        'expansion_alt': 'increasing usage of existing features',
    },
    'events': {
        'user_type': 'attendees and sponsors',
        'data_action': 'register, attend, or sponsor',
        'churn_a': 'a sponsor skipping one event is budget timing',
        'churn_b': 'a sponsor who attended 5 years straight not registering is churn',
        'expansion_signal': 'registering for new event categories',
        'expansion_alt': 'upgrading sponsorship tiers',
    },
    'cybersecurity': {
        'user_type': 'enterprise accounts',
        'data_action': 'scan, alert, or remediate',
        'churn_a': 'an account reducing scan frequency during a freeze is expected',
        'churn_b': 'an account disabling integrations is them evaluating a replacement',
        'expansion_signal': 'enabling new modules',
        'expansion_alt': 'expanding to new business units',
    },
    'default': {
        'user_type': 'active users',
        'data_action': 'engage, transact, or build',
        'churn_a': 'a user slowing down during a holiday period is seasonal',
        'churn_b': 'a power user reducing activity outside seasonal patterns is pre-churn',
        'expansion_signal': 'engaging with new product areas',
        'expansion_alt': 'deepening usage of existing features',
    },
}


def match_vertical(company_vertical):
    """Match a company's vertical to our mapping."""
    if not company_vertical:
        return VERTICAL_MAP['default']
    v = company_vertical.lower()
    for key in VERTICAL_MAP:
        if key == 'default':
            continue
        if key.lower() in v:
            return VERTICAL_MAP[key]
    # fuzzy matches
    if any(w in v for w in ['logistics', 'shipping', 'delivery', 'fleet', 'parcel', 'freight']):
        return VERTICAL_MAP['logistics']
    if any(w in v for w in ['fintech', 'payment', 'banking', 'financial', 'lending']):
        return VERTICAL_MAP['fintech']
    if any(w in v for w in ['hr tech', 'talent', 'recruiting', 'staffing', 'workforce']):
        return VERTICAL_MAP['HR tech']
    if any(w in v for w in ['learning', 'lms', 'education', 'edtech', 'training']):
        return VERTICAL_MAP['LMS']
    if any(w in v for w in ['telecom', 'mobile', 'connectivity']):
        return VERTICAL_MAP['telecom']
    if any(w in v for w in ['commerce', 'retail', 'ecommerce', 'shopping']):
        return VERTICAL_MAP['commerce']
    if any(w in v for w in ['cloud', 'hosting', 'infrastructure', 'server']):
        return VERTICAL_MAP['cloud']
    if any(w in v for w in ['marketplace', 'platform']):
        return VERTICAL_MAP['marketplace']
    if any(w in v for w in ['event', 'conference']):
        return VERTICAL_MAP['events']
    if any(w in v for w in ['security', 'cyber']):
        return VERTICAL_MAP['cybersecurity']
    if any(w in v for w in ['saas', 'software']):
        return VERTICAL_MAP['SaaS']
    return VERTICAL_MAP['default']


def classify_contact(title):
    """Classify a contact into a thread."""
    t = (title or '').lower()
    if any(w in t for w in ['strateg', 'analytics', 'data', 'intelligence', 'business develop']):
        return 'strategy'
    if any(w in t for w in ['customer success', 'retention', 'account management']):
        return 'cs'
    if any(w in t for w in ['revenue', 'cro', 'growth', 'sales']):
        return 'revenue'
    if any(w in t for w in ['product', 'cpo']):
        return 'revenue'  # product goes to revenue thread
    if any(w in t for w in ['marketing', 'cmo']):
        return 'strategy'  # marketing goes to strategy thread
    return 'revenue'  # default


TEMPLATES = {
    'strategy': {
        'subject': "{company}'s {user_type} churn signals",
        'emails': [
            # email 1
            """hey {first_name}

your {user_type} generate behavioral data every time they {data_action}

right now that data tells you what happened — not who's about to leave

we built something that reads those patterns across segments and flags churn 30-60 days before the numbers move

20 min to show you the signal layer and how it connects to your existing stack?

vlad
founder @ Beton (getbeton.ai)

ps: closing a similar pilot in travel-tech right now (3,900 employees) — happy to share the setup""",
            # email 2 - bump
            """hey {first_name}, did you have time to look into this?

vlad""",
            # email 3
            """hey {first_name}

the thing about {user_type} churn — same behavioral drop means different things depending on the segment

{churn_a}

{churn_b}

your analytics team can model basic churn but can't layer that distinction at scale — that's what we do

vlad""",
            # email 4
            """hey {first_name}

on the practical side — we deploy on-prem or cloud, sit on top of your DWH, and pipe signals into whatever CRM your team uses

$50K pilot, 1 month from signatures to validated signals on your own data

vlad""",
            # email 5 - breakup
            """hey {first_name}

last note — if {user_type} retention isn't a priority right now, understood

Beton is open source so your data team can run it without me on your own infra whenever the timing works

getbeton.ai/docs if you want to poke around

vlad""",
        ],
        'linkedin_connect': "hey {first_name} — sent you a note about behavioral churn signals for {company}'s {user_type}. thought it'd be worth connecting",
        'linkedin_dm': "{first_name} — following up on the email about {user_type} churn signals. worth a quick look if retention is on your radar this quarter",
    },
    'cs': {
        'subject': "saving {company}'s at-risk {user_type}",
        'emails': [
            """hey {first_name}

curious how your CS team decides which {user_type} need intervention right now

most platforms at {company}'s scale use activity frequency as a proxy — but that misses the behavioral shift that happens 30-60 days before someone actually churns

we detect those shifts across segments so your team intervenes with the right message at the right time

20 min to walk through how the signals work?

vlad
founder @ Beton (getbeton.ai)

ps: closing a similar pilot with a travel-tech platform (3,900 employees) — same pattern, different vertical""",
            """hey {first_name}, did you have time to look into this?

vlad""",
            """hey {first_name}

the difference between what we do and a churn dashboard — we backtest every signal against new data with a pipeline of statistical tests before flagging it

your team doesn't chase false positives

the output is a prioritized list of at-risk {user_type} with specific reasons why each one is flagged

vlad""",
            """hey {first_name}

on deployment — we sit on top of your existing DWH, no data migration

your CS team gets signals in whatever tool they already use

$50K pilot, 1 month from signatures to validated signals

vlad""",
            """hey {first_name}

closing the loop — if retention intelligence isn't on your roadmap this quarter, no worries

Beton is open source so your team can spin it up on your own infra whenever the timing is right

getbeton.ai

vlad""",
        ],
        'linkedin_connect': "hey {first_name} — sent a quick note about detecting at-risk {user_type} earlier. happy to connect",
        'linkedin_dm': "{first_name} — following up on the email about at-risk {user_type}. worth a look if retention is a priority this quarter",
    },
    'revenue': {
        'subject': "{company}'s expansion signals",
        'emails': [
            """hey {first_name}

{user_type} who are about to expand their usage with {company} show specific behavioral patterns weeks before they actually do

right now those signals sit in your data warehouse and nobody's reading them

we surface expansion-ready accounts so your reps know who to call and when

20 min to show you how the signal detection works?

vlad
founder @ Beton (getbeton.ai)

ps: closing a pilot with a travel-tech platform on expansion + churn signals — similar setup""",
            """hey {first_name}, did you have time to look into this?

vlad""",
            """hey {first_name}

the challenge with expansion signals — {expansion_signal} looks different from {expansion_alt}

both are expansion but your reps need different conversations for each

we classify the signal type so reps show up with the right pitch

vlad""",
            """hey {first_name}

signals pipe into your CRM as scored accounts with context your reps can act on

no new tool to learn, no dashboard to check

$50K pilot, 1 month from signatures to validated signals

vlad""",
            """hey {first_name}

last one from me — if expansion intelligence for {user_type} isn't top of mind, understood

Beton is open source — your data team can run the signal layer without us whenever you're ready

getbeton.ai/docs

vlad""",
        ],
        'linkedin_connect': "hey {first_name} — dropped you a note about expansion signals in {company}'s {user_type} data. let's connect",
        'linkedin_dm': "{first_name} — following up on the email about expansion signals. worth a quick look if growth is on your radar",
    },
}


def generate_emails(contact, vertical_info, thread):
    """Generate all emails for one contact."""
    template = TEMPLATES[thread]
    first_name = contact['name'].split()[0] if contact.get('name') else 'there'

    vars = {
        'first_name': first_name.lower(),
        'company': contact['company'],
        'user_type': vertical_info['user_type'],
        'data_action': vertical_info['data_action'],
        'churn_a': vertical_info['churn_a'],
        'churn_b': vertical_info['churn_b'],
        'expansion_signal': vertical_info['expansion_signal'],
        'expansion_alt': vertical_info['expansion_alt'],
    }

    subject = template['subject'].format(**vars)
    emails = []

    # Day offsets per thread
    day_offsets = {
        'strategy': [0, 3, 5, 8, 12],
        'cs': [2, 5, 7, 10, 14],
        'revenue': [4, 7, 9, 12, 15],
    }

    for i, body_template in enumerate(template['emails']):
        body = body_template.format(**vars)
        step = i + 1
        day = day_offsets[thread][i]
        sub = subject if step == 1 else f"re: {subject}"

        emails.append({
            'company': contact['company'],
            'domain': contact['domain'],
            'emp': contact.get('emp', 0),
            'contact_name': contact['name'],
            'contact_email': contact['email'],
            'contact_title': contact['title'],
            'contact_linkedin': contact.get('linkedin', ''),
            'thread': thread,
            'step': step,
            'day': day,
            'subject': sub,
            'body': body,
            'wave': contact.get('wave', 'wave1'),
        })

    # LinkedIn actions
    li_connect = template['linkedin_connect'].format(**vars)
    li_dm = template['linkedin_dm'].format(**vars)

    emails.append({
        'company': contact['company'], 'domain': contact['domain'],
        'emp': contact.get('emp', 0),
        'contact_name': contact['name'], 'contact_email': '',
        'contact_title': contact['title'],
        'contact_linkedin': contact.get('linkedin', ''),
        'thread': thread, 'step': 'li_connect',
        'day': day_offsets[thread][0],  # same day as email 1
        'subject': '', 'body': li_connect,
        'wave': contact.get('wave', 'wave1'),
    })
    emails.append({
        'company': contact['company'], 'domain': contact['domain'],
        'emp': contact.get('emp', 0),
        'contact_name': contact['name'], 'contact_email': '',
        'contact_title': contact['title'],
        'contact_linkedin': contact.get('linkedin', ''),
        'thread': thread, 'step': 'li_dm',
        'day': day_offsets[thread][2],  # same day as email 3
        'subject': '', 'body': li_dm,
        'wave': contact.get('wave', 'wave1'),
    })

    return emails


def load_contacts():
    """Load all revealed contacts including gap fill."""
    combined_path = os.path.join(BASE, 'all_campaign_contacts.json')
    if os.path.exists(combined_path):
        with open(combined_path) as f:
            return json.load(f)

    contacts = []
    for fname, wave_default in [
        ('wave1_revealed_contacts.json', 'wave1'),
        ('waves234_revealed_contacts.json', None),
        ('t1_contacts.json', 'wave1'),
        ('gap_fill_results.json', None),
    ]:
        path = os.path.join(BASE, fname)
        if os.path.exists(path):
            with open(path) as f:
                for c in json.load(f):
                    if wave_default and 'wave' not in c:
                        c['wave'] = wave_default
                    contacts.append(c)

    seen = set()
    deduped = []
    for c in contacts:
        email = c.get('email', '')
        if email and email not in seen and email not in ('None', '(no email)'):
            seen.add(email)
            deduped.append(c)
    return deduped


def load_verticals():
    """Load company verticals from campaign waves."""
    verticals = {}
    waves_path = os.path.join(BASE, 'campaign_waves.json')
    if os.path.exists(waves_path):
        with open(waves_path) as f:
            waves = json.load(f)
            # waves don't have vertical info — need to get from apollo results

    for fname in ['apollo_6sense_300_results.json', 'apollo_campaign_300_results.json']:
        fpath = os.path.join(BASE, fname)
        if os.path.exists(fpath):
            with open(fpath) as f:
                for r in json.load(f):
                    org = r.get('org', {})
                    if org and not org.get('error'):
                        verticals[r['domain']] = org.get('industry', '')

    return verticals


def main():
    contacts = load_contacts()
    verticals = load_verticals()

    # Filter to contacts with email
    with_email = [c for c in contacts if c.get('email') and c['email'] not in ('', '(no email)', 'None', None)]

    print(f"Total contacts: {len(contacts)}")
    print(f"With email: {len(with_email)}")

    all_emails = []
    companies_processed = set()

    for contact in with_email:
        domain = contact.get('domain', '')
        industry = verticals.get(domain, '')
        vertical_info = match_vertical(industry)
        thread = classify_contact(contact.get('title', ''))

        emails = generate_emails(contact, vertical_info, thread)
        all_emails.extend(emails)
        companies_processed.add(domain)

    # Write CSV
    output_path = os.path.join(BASE, 'campaign-big-b2b', 'all_emails.csv')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'wave', 'company', 'domain', 'emp', 'contact_name', 'contact_email',
            'contact_title', 'contact_linkedin', 'thread', 'step', 'day',
            'subject', 'body'
        ])
        writer.writeheader()
        writer.writerows(all_emails)

    # Stats
    email_only = [e for e in all_emails if isinstance(e['step'], int)]
    li_only = [e for e in all_emails if isinstance(e['step'], str)]

    print(f"\nGenerated:")
    print(f"  Companies: {len(companies_processed)}")
    print(f"  Contacts: {len(with_email)}")
    print(f"  Email steps: {len(email_only)}")
    print(f"  LinkedIn steps: {len(li_only)}")
    print(f"  Total actions: {len(all_emails)}")
    print(f"\nSaved to: {output_path}")

    # Per-wave breakdown
    from collections import Counter
    wave_counts = Counter(e['wave'] for e in all_emails)
    thread_counts = Counter(e['thread'] for e in all_emails)
    print(f"\nBy wave: {dict(wave_counts)}")
    print(f"By thread: {dict(thread_counts)}")


if __name__ == '__main__':
    main()
